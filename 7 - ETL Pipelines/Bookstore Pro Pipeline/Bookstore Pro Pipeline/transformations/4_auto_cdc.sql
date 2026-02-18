CREATE OR REFRESH STREAMING TABLE timpomaville.bookstore_etl_pro.books_silver;

CREATE FLOW books_flow
AS AUTO CDC INTO timpomaville.bookstore_etl_pro.books_silver
FROM STREAM(books_raw)
KEYS (book_id)
SEQUENCE BY updated
COLUMNS * EXCEPT (updated)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW timpomaville.bookstore_etl_pro.current_books
AS SELECT book_id, title, author, price
   FROM timpomaville.bookstore_etl_pro.books_silver
   WHERE __END_AT IS NULL;

CREATE OR REFRESH STREAMING TABLE timpomaville.bookstore_etl_pro.books_sales(
  CONSTRAINT valid_subtotal EXPECT(b.book.subtotal = b.book.quantity * c.price) ON VIOLATION DROP ROW,
  CONSTRAINT valid_total EXPECT(total BETWEEN 0 AND 100000) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_date EXPECT(order_timestamp <= current_date() AND year(order_timestamp) >= 2020)
) AS SELECT *
   FROM STREAM(timpomaville.bookstore_etl_pro.orders_silver) AS o,
        LATERAL EXPLODE(o.books) AS b(book)
   INNER JOIN timpomaville.bookstore_etl_pro.current_books AS c
   ON b.book.book_id = c.book_id;

CREATE OR REFRESH MATERIALIZED VIEW timpomaville.bookstore_etl_pro.authors_stats
COMMENT "Aggregated statistics of book sales per author in 5-minute windows"
AS SELECT
  author,
  window.start AS window_start,
  window.end AS window_end,
  COUNT(order_id) AS orders_count,
  AVG(quantity) AS avg_quantity
FROM timpomaville.bookstore_etl_pro.books_sales
GROUP BY
  author,
  window(order_timestamp, '5 minutes', '5 minutes', '2 minutes')
ORDER BY
  window_start;
