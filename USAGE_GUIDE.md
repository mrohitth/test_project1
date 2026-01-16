# Pipeline Usage Guide

## Quick Start

### Local Development

#### 1. Clone the Repository
```bash
git clone https://github.com/mrohitth/test_project1.git
cd test_project1
```

#### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 3. Run the Simple Pipeline (Recommended for Testing)
```bash
python src/run_pipeline_simple.py
```

This will:
- Load sample data from `data/raw/sample_data.csv`
- Apply transformations and cleaning
- Load results into DuckDB database
- Generate metrics and reports in the `logs/` directory

#### 4. Run the Dashboard
```bash
streamlit run dashboard/app.py
```

The dashboard will open in your browser with interactive visualizations.

---

## Google Colab Usage

### Quick Start with Colab

1. **Open the Notebook**: [Pipeline_Validation_Colab.ipynb](https://colab.research.google.com/github/mrohitth/test_project1/blob/copilot/fix-pipeline-refactor-errors/Pipeline_Validation_Colab.ipynb)

2. **Run All Cells**: Click `Runtime` → `Run all`

3. **Follow the Prompts**:
   - Enter AWS credentials when prompted (if using S3)
   - Configure data source (S3, local, or generate sample)
   - Review metrics and visualizations

### Detailed Colab Instructions

See [COLAB_README.md](COLAB_README.md) for complete instructions.

---

## Pipeline Components

### 1. Data Loading (`load_data.py`)

**Purpose**: Load data from various sources (local files, S3)

**Usage**:
```python
from src.load_data import load_data

# Load from local file
data = load_data('data/raw/mydata.csv')

# Load from S3
data = load_data('s3://my-bucket/data/input.csv', source_type='s3')
```

**Supported Formats**:
- CSV files (`.csv`)
- JSON files (`.json`)
- Pickle files (`.pkl`, `.pickle`)

### 2. Data Transformation (`transform_data_pandas.py`)

**Purpose**: Clean, transform, and enrich data

**Usage**:
```python
from src.transform_data_pandas import PandasDataTransformer

transformer = PandasDataTransformer()

# Remove duplicates
clean_data = transformer.remove_duplicates(df)

# Handle missing values
clean_data = transformer.handle_missing_values(clean_data, strategy='median')

# Create date features
enriched_data = transformer.create_date_features(clean_data, 'timestamp_column')

# Get quality report
quality_report = transformer.get_data_quality_report(enriched_data)
```

**Available Methods**:
- `remove_duplicates()` - Remove duplicate rows
- `handle_missing_values()` - Fill or drop missing values
- `clean_string_columns()` - Clean and standardize text
- `remove_outliers()` - Remove statistical outliers
- `create_date_features()` - Extract date components
- `create_numerical_features()` - Create derived features
- `simple_aggregation()` - Group and aggregate data
- `get_data_quality_report()` - Generate quality metrics

### 3. Database Loading (`load_to_db_simple.py`)

**Purpose**: Load data into databases (DuckDB or PostgreSQL)

**Usage**:
```python
from src.load_to_db_simple import SimpleDatabaseLoader

# DuckDB (recommended for development)
loader = SimpleDatabaseLoader('duckdb', {'database': 'mydata.db'})

# PostgreSQL (for production)
loader = SimpleDatabaseLoader('postgresql', {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'mypassword',
    'database': 'mydb'
})

# Load data
stats = loader.load_dataframe(df, 'my_table', mode='overwrite')

# Query data
result = loader.execute_query('SELECT * FROM my_table LIMIT 10')

loader.close()
```

### 4. Pipeline Orchestrator (`run_pipeline_simple.py`)

**Purpose**: Execute complete ETL pipeline

**Usage**:
```python
from src.run_pipeline_simple import SimplePipelineOrchestrator

config = {
    'data_source': 'data/raw/input.csv',
    'source_type': 'local',
    'db_type': 'duckdb',
    'db_params': {'database': 'output.db'},
    'table_name': 'processed_data'
}

orchestrator = SimplePipelineOrchestrator(config)
metrics = orchestrator.run()

print(f"Pipeline status: {metrics['overall_status']}")
print(f"Rows processed: {metrics['total_rows_transformed']}")
```

### 5. S3 Data Loader (`s3_loader.py`)

**Purpose**: Interact with AWS S3 buckets

**Usage**:
```python
from src.s3_loader import S3DataLoader

loader = S3DataLoader(
    aws_access_key_id='YOUR_KEY',
    aws_secret_access_key='YOUR_SECRET',
    region_name='us-east-1'
)

# List objects
objects = loader.list_objects('my-bucket', prefix='data/')

# Load CSV
df = loader.load_csv_from_s3('my-bucket', 'data/input.csv')

# Upload DataFrame
loader.upload_dataframe(df, 'my-bucket', 'output/results.csv')
```

### 6. Dashboard (`dashboard/app.py`)

**Purpose**: Interactive data visualization

**Features**:
- Real-time data filtering
- Multiple visualization types
- Data export (CSV/JSON)
- Performance metrics
- Aggregation views

**Launch**:
```bash
streamlit run dashboard/app.py
```

Then open http://localhost:8501 in your browser.

---

## AWS S3 Setup

### Prerequisites
1. AWS Account
2. IAM User with S3 permissions
3. Access Key ID and Secret Access Key

### Required IAM Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name/*",
        "arn:aws:s3:::your-bucket-name"
      ]
    }
  ]
}
```

### Environment Variables
Create a `.env` file:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
```

Or set in your shell:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

---

## Configuration Options

### Pipeline Configuration

```python
config = {
    # Data Source
    'data_source': 'data/raw/input.csv',  # File path or S3 URI
    'source_type': 'local',  # 'local' or 's3'
    
    # Database
    'db_type': 'duckdb',  # 'duckdb' or 'postgresql'
    'db_params': {
        'database': 'pipeline.db'  # For DuckDB
        # For PostgreSQL:
        # 'host': 'localhost',
        # 'port': 5432,
        # 'user': 'postgres',
        # 'password': 'password',
        # 'database': 'mydb'
    },
    'table_name': 'processed_data',
    
    # S3 Configuration (if using S3)
    'aws_access_key_id': 'YOUR_KEY',
    'aws_secret_access_key': 'YOUR_SECRET',
    'region_name': 'us-east-1'
}
```

---

## Troubleshooting

### Common Issues

#### Issue: ModuleNotFoundError: No module named 'pyspark'
**Solution**: Use the pandas-based modules instead:
- Use `transform_data_pandas.py` instead of `transform_data.py`
- Use `load_to_db_simple.py` instead of `load_to_db.py`
- Use `run_pipeline_simple.py` instead of `run_pipeline.py`

#### Issue: AWS credentials not found
**Solution**: Set environment variables or pass credentials explicitly:
```python
loader = S3DataLoader(
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret'
)
```

#### Issue: Database connection failed
**Solution**: 
- For DuckDB: Ensure write permissions to the database file location
- For PostgreSQL: Verify connection parameters and network access

#### Issue: Out of memory
**Solution**: 
- Process data in smaller chunks
- Use DuckDB instead of loading all data into pandas
- Filter data before loading

---

## Best Practices

### 1. Data Loading
- Always validate data after loading
- Use try-except blocks for error handling
- Log data source and row counts

### 2. Data Transformation
- Apply transformations in logical order
- Document business rules
- Generate quality reports before and after

### 3. Database Loading
- Use 'overwrite' mode for initial loads
- Use 'append' mode for incremental updates
- Always verify row counts after loading

### 4. Pipeline Execution
- Run with sample data first
- Monitor logs for errors
- Review metrics reports

### 5. Production Deployment
- Use PostgreSQL for production databases
- Store credentials in environment variables
- Implement retry logic for S3 operations
- Set up monitoring and alerting

---

## Examples

### Example 1: Basic ETL Pipeline
```python
from src.load_data import load_data
from src.transform_data_pandas import PandasDataTransformer
from src.load_to_db_simple import SimpleDatabaseLoader

# Load
df = load_data('data/raw/sales.csv')

# Transform
transformer = PandasDataTransformer()
df_clean = transformer.remove_duplicates(df)
df_clean = transformer.handle_missing_values(df_clean)

# Load
loader = SimpleDatabaseLoader('duckdb', {'database': 'sales.db'})
stats = loader.load_dataframe(df_clean, 'sales_data')
loader.close()

print(f"Loaded {stats['output_row_count']} rows")
```

### Example 2: S3 to Database Pipeline
```python
from src.s3_loader import S3DataLoader
from src.transform_data_pandas import PandasDataTransformer
from src.load_to_db_simple import SimpleDatabaseLoader

# Load from S3
s3_loader = S3DataLoader()
df = s3_loader.load_csv_from_s3('my-bucket', 'data/input.csv')

# Transform
transformer = PandasDataTransformer()
df_clean = transformer.remove_duplicates(df)

# Load to database
db_loader = SimpleDatabaseLoader('duckdb', {'database': 'output.db'})
db_loader.load_dataframe(df_clean, 'processed_data')
db_loader.close()
```

### Example 3: Complete Pipeline with Metrics
```python
from src.run_pipeline_simple import SimplePipelineOrchestrator

config = {
    'data_source': 's3://my-bucket/data/input.csv',
    'source_type': 's3',
    'db_type': 'duckdb',
    'db_params': {'database': 'pipeline.db'},
    'table_name': 'results'
}

orchestrator = SimplePipelineOrchestrator(config)
metrics = orchestrator.run()

print(f"Status: {metrics['overall_status']}")
print(f"Duration: {metrics['total_duration_seconds']:.2f}s")
print(f"Rows: {metrics['total_rows_transformed']}")
```

---

## Additional Resources

- **Colab Notebook**: See `Pipeline_Validation_Colab.ipynb`
- **Colab Guide**: See `COLAB_README.md`
- **API Documentation**: See inline docstrings in each module
- **Dashboard Guide**: See `dashboard/README.md` (if available)

---

## Support

For issues or questions:
1. Check this guide and the Colab README
2. Review module docstrings
3. Check logs in the `logs/` directory
4. Open an issue on GitHub
