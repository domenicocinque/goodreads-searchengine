# GoodReads Search Engine

This is a search engine for GoodReads books. We first scrape the data from GoodReads website and then use the data to build a recommender system.

## TODO

- [ ] Insert a method to update the database
- [ ] Consider other ways to store the data other than a csv file. Maybe a database such as SQLite or MongoDB.
- [ ] Show the book cover and the rating in the search results.
- [ ] A possible interesting improvement could be to use Whoosh to have a set of candidates with an OR query and then use BERT or FastText embeddings to rank the results.
- [ ] Can we add word embeddings to the posting list? This could avoid the need to load the embeddings every time we want to search.
