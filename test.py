import fetcher

reviews = fetcher.get_item_reviews("MLA723647586",100)

print(len(reviews))
print(reviews[0].content)
