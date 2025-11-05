# Germany ETL Portfolio — Week 2

**Junior Data Engineer | Python • Pandas • PostgreSQL • Neon**

---

## ETL Pipeline: CSV → Transform → PostgreSQL

- **Extract**: 30 sales records  
- **Transform**: Add 10% tax + GrandTotal, filter `North` region  
- **Load**: Neon serverless DB (Singapore)

---

### Run in 5 seconds:
```bash
pip install -r requirements.txt
python csv_etl_pipeline.py