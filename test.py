import fetcher

reviews = fetcher.get_item_reviews("MLA723647586")

print(len(reviews))
print(review[0].content)
