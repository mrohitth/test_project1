# Google Colab Validation Notebook

## Overview
This Google Colab notebook provides a comprehensive validation and showcase of the data pipeline functionality with full AWS S3 integration.

## Features
- ✅ AWS S3 data loading
- ✅ Complete ETL pipeline execution
- ✅ Data transformation with pandas
- ✅ Comprehensive metrics calculation
- ✅ Interactive visualizations
- ✅ Export results to CSV/JSON
- ✅ Upload results back to S3

## Usage Instructions

### 1. Open in Google Colab
Click here to open: [Open Pipeline_Validation_Colab.ipynb in Google Colab](https://colab.research.google.com/github/mrohitth/test_project1/blob/copilot/fix-pipeline-refactor-errors/Pipeline_Validation_Colab.ipynb)

### 2. Prepare Your AWS Credentials
You'll need:
- AWS Access Key ID
- AWS Secret Access Key
- AWS Region (default: us-east-1)
- S3 Bucket name (if using S3 data source)
- S3 Key/path to your data file

### 3. Run the Notebook
Follow the cells in order:

1. **Setup and Installation** - Clones repo and installs dependencies
2. **AWS Configuration** - Enter your AWS credentials securely
3. **Import Modules** - Loads required libraries
4. **Configure Data Source** - Choose S3, local file, or generate sample data
5. **Load Data** - Loads data from your chosen source
6. **Transform Data** - Applies cleaning and transformation pipeline
7. **Calculate Metrics** - Generates comprehensive statistics
8. **Visualizations** - Creates interactive charts and graphs
9. **Advanced Analytics** - Additional insights and analysis
10. **Export Results** - Download processed data and metrics
11. **Upload to S3** (Optional) - Save results back to S3
12. **Dashboard** (Optional) - Launch interactive Streamlit dashboard
13. **Final Summary** - Complete execution report

## Data Source Options

### Option 1: AWS S3
- Best for production use
- Requires AWS credentials
- Can handle large datasets
- Example: `s3://my-bucket/data/input.csv`

### Option 2: Local File
- Good for testing with sample data
- Upload a file to Colab using the file upload widget
- Example: `data/raw/my_data.csv`

### Option 3: Generate Sample Data
- Perfect for testing and demonstrations
- No external data required
- Generates realistic taxi trip data
- Configurable size (100 to 50,000 records)

## Expected Outputs

### Metrics
- Total trips processed
- Revenue statistics
- Average fare, distance, and speed
- Data quality scores
- Transformation success rate

### Visualizations
- Trip distribution by vendor
- Revenue analysis
- Distance and fare distributions
- Hourly trip patterns
- Payment type breakdown
- Location-based insights

### Exports
- `transformed_data_YYYYMMDD_HHMMSS.csv` - Cleaned and transformed data
- `pipeline_metrics_YYYYMMDD_HHMMSS.json` - Complete metrics in JSON format
- `vendor_summary_YYYYMMDD_HHMMSS.csv` - Vendor performance summary

## Troubleshooting

### Issue: AWS Credentials Error
**Solution:** Double-check your AWS credentials. Make sure your IAM user has S3 read permissions.

### Issue: Module Not Found
**Solution:** Re-run the installation cell. Ensure all packages are installed correctly.

### Issue: S3 Access Denied
**Solution:** Verify bucket permissions and check that your IAM policy allows `s3:GetObject` action.

### Issue: Out of Memory
**Solution:** Reduce the sample size or use data filtering to process less data at once.

## Performance Tips

1. **For Large Datasets**: Use S3 filtering or select specific columns
2. **For Faster Execution**: Use sample data generation with smaller sizes
3. **For Better Visualizations**: Filter data to specific time periods or vendors

## Support

For issues or questions:
1. Check the main project README
2. Review the inline documentation in the notebook
3. Open an issue on GitHub

## Version History

- **v1.0** (2026-01-16): Initial release with full S3 integration and interactive visualizations
