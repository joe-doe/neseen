# neseen - **NE**w **SE**arch **EN**gine
## Another href crawler.

It's an infinite loop following these steps (roughly):

0. Set an initial url
1. Crawl url
2. Find hrefs within
3. Store them along with metadata and body in a datastore
4. Find next not parsed url from datastore
5. GOTO step 1