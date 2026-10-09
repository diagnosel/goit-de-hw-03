# CSV sources

The labels in the homework links mix up products and purchases. We identified each file by its header and the Drive file name.

| Local file | Header | Original source |
| --- | --- | --- |
| `data/users.csv` | `user_id,name,age,email` | https://drive.google.com/file/d/1GN8o8vYP8LLj3A0UWkvbgnf0YjcF3PkV/view?usp=drive_link |
| `data/products.csv` | `product_id,product_name,category,price` | https://drive.google.com/file/d/1im5cGVl1z8ejypTyNJZSQ2lqMGh6tTFN/view?usp=sharing |
| `data/purchases.csv` | `purchase_id,user_id,product_id,date,quantity` | https://drive.google.com/file/d/1cDLTc0KYQfnsDWHLR0cJkF5puBpAo2Uh/view?usp=sharing |

The downloaded files are unchanged. Their SHA-256 checksums are stored in `outputs/audit.json` after a successful run.

## Official references

- Setup and supported versions: https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html
- Data types: https://spark.apache.org/docs/4.0.1/sql-ref-datatypes.html
- CSV reader: https://spark.apache.org/docs/4.0.1/sql-data-sources-csv.html

No live customer data was used. The user file contains `User_N` names and `userN@example.com` addresses. We keep the source rows unchanged for the homework. Public release still needs the owner's review and approval.
