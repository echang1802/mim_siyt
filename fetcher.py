import argparse
import math
from pathlib import Path

import pyarrow
import pyarrow.parquet
import requests

# Some useful constants
MELI_BASE_URL = 'https://api.mercadolibre.com'

MELI_ITEMS_RESOURCE = 'sites/MLA/search'
MELI_REVIEWS_RESOURCE = 'reviews/item'


class Item:
    """
    Models a published Item
    """
    def __init__(self, key, category, title, reviews=None):
        self.key = key
        self.category = category
        self.title = title
        self.reviews = reviews

class Review:
    """
    Models an Item's review
    """
    def __init__(self, key, title, content, rate, likes, dislikes):
        self.key = key
        self.title = title
        self.content = content
        self.rate = rate
        self.likes = likes
        self.dislikes = dislikes

def get_resource(resource, subresource='', params = None):
    """
    Util method to get an API resource

    :param resource: resource to get, e.g. reviews
    :param subresource: subresource if needed, e.g. a specific item "MLA723647586"
    :param params: dictionary with the url query params, e.g. {'offset': 100}
    :return: the response json content loaded into a dictionary
    :raises: HTTPError if the response status is between 400 and 599
    """
    # TODO construir una URL con 'MELI_BASE_URL', 'resource', 'subresource' y 'params'
    subresource = ("/" if subresource != "" else "") + subresource
    url = MELI_BASE_URL + "/" + resource + subresource
    # TODO y luego ir a buscar los contenidos de dicha URL
    data = requests.get(url, params = params)

    # TODO importante tener en cuenta el manejo de errores, en caso de que falle el request
    if data.status_code >= 400 and data.status_code <= 599:
        return {}

    return data.json()

def get_resource_paginated(resource, subresource='', params=None, page_limit=50):
    """
    Util method to get an API resource handling paging if needed

    :param resource: resource to get, e.g. reviews
    :param subresource: subresource if needed, e.g. a specific item "MLA723647586"
    :param params: dictionary with the url query params, e.g. {'offset': 100}
    :param page_limit: max amount of objects per page (note if the API max limit is lower, that amount will be
        returned instead)
    :return: generator returning one page at a time when iterated on (this will do a lazy request of pages)
    :raises: HTTPError if any request response status is between 400 and 599
    """
    params = params or {}
    params.update({
        'limit': page_limit
    })
    data = get_resource(resource, subresource, params)
    yield data

    if 'paging' in data:
        paging = data['paging']
        total = paging['total']
        limit = paging['limit']
        for page in range(1, math.ceil(total/limit)):
            params.update({
                'offset': page*limit
            })
            data = get_resource(resource, subresource, params)
            yield data

def store_items_with_reviews(items, category, page_num, output_directory):
    """
    Persist Items with Reviews
    :param items: list of Items to persist
    :param category: category of the items
    :param page_num: number of page the items come from
    :param output_directory: directory where to store the data
    """
    # TODO aquí debemos construir una vista columnar de las reviews
    vista = {
        "item_key"  : [],
        "category" : [],
        "item_title" : [],
        "review_key" : [],
        "review_title" : [],
        "review_content" : [],
        "review_rate" : [],
        "review_likes" : [],
        "review_dislikes" : []
    }
    for i in items:
        for r in i.reviews:
            vista["item_key"].append(i.key)
            vista["category"].append(i.category)
            vista["item_title"].append(i.title)
            vista["review_key"].append(r.key)
            vista["review_title"].append(r.title)
            vista["review_content"].append(r.content)
            vista["review_rate"].append(r.rate)
            vista["review_likes"].append(r.likes)
            vista["review_dislikes"].append(r.dislikes)

    vista = pyarrow.Table.from_pydict(vista)

    # TODO luego debemos guardarla en 'output_directory' en formato parquet
    # TODO el nombre de archivo debe reflejar categoría y número de página
    filename = output_directory + "/" + category + str(page_num) + ".parquet"
    pyarrow.parquet.write_table(vista,  filename)

def get_item_reviews(item_id, max_reviews_per_item):
    """
    Get all the Item's Reviews

    :param item_id: item id to get reviews of, e.g. "MLA723647586"
    :return: list of Reviews
    """

    # TODO aquí deberíamos retornar un objeto Review por cada review del ítem
    reviews = []
    ready = False
    while not ready:
        pages =  get_resource_paginated(MELI_REVIEWS_RESOURCE, subresource=item_id, params=None, page_limit=50)
        for i, page in enumerate(pages):
            if not "reviews" in page.keys():
                return reviews
            for review in page["reviews"]:
                reviews.append(Review(review["id"], review["title"], review["content"], review["rate"], review["likes"], review["dislikes"]))
                if len(reviews) == max_reviews_per_item:
                    return reviews
    return reviews



def get_page_items(page, reviews_goal, max_reviews_per_item):
    items = []
    reviews_count = 0
    for item in page['results']:
        reviews = get_item_reviews(item['id'], max_reviews_per_item)
        if len(reviews) == 0:
            # We only care about items with reviews
            continue

        print(f'\tGot {len(reviews)} reviews for item {item["id"]}')
        reviews_count += len(reviews)
        items.append(Item(key=item['id'], category=item['category_id'], title=item['title'], reviews=reviews))

        # If we have reached the number of reviews goal for this category then we stop fetching
        if reviews_count >= reviews_goal:
            break

    return items, reviews_count


def visit_items_with_reviews(category, output_directory, reviews_goal, max_reviews_per_item):
    """
    Visit all the category's Items, executing a callback on each page of results

    :param category: category to visit, e.g. "MLA5725"
    :param output_directory: directory where to store the data
    :param reviews_goal: target number of reviews to fetch for this category, once reached stop fetching
    :param max_reviews_per_item: maximum number of reviews to fetch per item
    """
    params = {
        'category': category
    }
    total_reviews = 0
    print(f'Getting items from category {category}')
    pages = get_resource_paginated(MELI_ITEMS_RESOURCE, params=params)
    for i, page in enumerate(pages):
        items, reviews_count = get_page_items(page, reviews_goal=reviews_goal - total_reviews,
                                              max_reviews_per_item=max_reviews_per_item)
        total_reviews += reviews_count

        store_items_with_reviews(items, category, i, output_directory)

        if total_reviews >= reviews_goal:
            break


def main():
    # Parse the command line arguments
    parser = argparse.ArgumentParser(description='MeLi data fetcher')
    parser.add_argument('--category', required=True, help='Product category to fetch')
    parser.add_argument('--output-directory', default='reviews', help='Directory where to store the data')
    parser.add_argument('--reviews-goal', type=int, default=50000, help='Target number of reviews to fetch')
    parser.add_argument('--max-reviews-per-item', type=int, default=100, help='Maximum number of reviews per item')

    args = parser.parse_args()

    # Create output directory if it does not yet exist
    Path(args.output_directory).mkdir(exist_ok=True)

    # Visit all the Category Reviews
    visit_items_with_reviews(args.category, args.output_directory, args.reviews_goal, args.max_reviews_per_item)


if __name__ == '__main__':
    main()
