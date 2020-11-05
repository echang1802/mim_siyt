import pyarrow
import pyarrow.parquet

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


items = []
idRev = 1
for i in range(10):
    rev = []
    for j in range(10):
        rev.append(Review(idRev,"title_" + str(i) + "_" + str(j),"content for " + str(j),j,j,j))
    items.append(Item(i,"items","item_" + str(i),rev))


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
        "category" : [],
        "item_title" : [],
        "review_title" : [],
        "review_content" : [],
        "review_rate" : [],
        "review_likes" : [],
        "review_dislikes" : []
    }
    for i in items:
        for r in i.reviews:
            vista["category"].append(i.category)
            vista["item_title"].append(i.title)
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

store_items_with_reviews(items, "test", 1, "")
