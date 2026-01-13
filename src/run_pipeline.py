#!/usr/bin/env python3
"""
ETL Pipeline Orchestrator
Executes the complete ETL pipeline with comprehensive metrics and logging.

This module orchestrates the execution of:
1. load_data.py - Extract raw data from source systems
2. transform_data.py - Transform and validate data
3. load_to_db.py - Load processed data to database

Features:
- Timing metrics for each stage
- Row counts and data quality metrics
- Comprehensive JSON logging for dashboard visualization
- Error handling and rollback capabilities
- Pipeline stage tracking and reporting
"""

import json
import logging
import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import traceback


class PipelineStage(Enum):
    """Pipeline execution stages."""
    INITIALIZATION = "initialization"
    DATA_LOADING = "data_loading"
    DATA_TRANSFORMATION = "data_transformation"
    DATA_VALIDATION = "data_validation"
    DATABASE_LOADING = "database_loading"
    COMPLETION = "completion"
    FAILED = "failed"


@dataclass
class StageMetrics:
    """Metrics for a pipeline stage."""
    stage_name: str
    start_time: str
    end_time: str
    duration_seconds: float
    status: str
    row_count_input: int = 0
    row_count_output: int = 0
    row_count_processed: int = 0
    row_count_failed: int = 0
    error_message: str = None
    error_traceback: str = None
    memory_usage_mb: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return asdict(self)


@dataclass
class PipelineMetrics:
    """Aggregated pipeline metrics."""
    pipeline_id: str
    start_time: str
    end_time: str
    total_duration_seconds: float
    overall_status: str
    stages: List[Dict[str, Any]]
    total_rows_loaded: int = 0
    total_rows_transformed: int = 0
    total_rows_failed: int = 0
    total_rows_loaded_to_db: int = 0
    success_rate: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return asdict(self)


class PipelineOrchestrator:
    """Orchestrates the complete ETL pipeline execution."""
    
    def __init__(self, project_root: str = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            project_root: Root directory of the project
        """
        self.project_root = Path(project_root) if project_root else Path(__file__).parent.parent
        self.src_dir = self.project_root / "src"
        self.logs_dir = self.project_root / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        
        # Pipeline state
        self.pipeline_id = self._generate_pipeline_id()
        self.pipeline_start_time = datetime.utcnow()
        self.stage_metrics: List[StageMetrics] = []
        self.pipeline_state = {
            "data_loaded": False,
            "data_transformed": False,
            "data_loaded_to_db": False,
            "intermediate_files": []
        }
        
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
    def _generate_pipeline_id(self) -> str:
        """Generate unique pipeline execution ID."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"pipeline_{timestamp}"
    
    def _setup_logging(self):
        """Setup both file and console logging."""
        log_file = self.logs_dir / f"{self.pipeline_id}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0
    
    def _log_stage_json(self, metrics: StageMetrics):
        """Log stage metrics as JSON."""
        json_log_file = self.logs_dir / f"{self.pipeline_id}_metrics.jsonl"
        with open(json_log_file, 'a') as f:
            f.write(json.dumps(metrics.to_dict()) + '\n')
    
    def _execute_stage(self, stage: PipelineStage, script_name: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute a pipeline stage.
        
        Args:
            stage: Pipeline stage identifier
            script_name: Name of the Python script to execute
            
        Returns:
            Tuple of (success, stage_output)
        """
        stage_start = time.time()
        start_time = datetime.utcnow().isoformat()
        
        self.logger.info(f"Starting stage: {stage.value}")
        
        stage_result = {
            "rows_input": 0,
            "rows_output": 0,
            "rows_processed": 0,
            "rows_failed": 0
        }
        
        try:
            script_path = self.src_dir / script_name
            
            if not script_path.exists():
                raise FileNotFoundError(f"Script not found: {script_path}")
            
            # Execute the stage script
            self.logger.info(f"Executing: {script_path}")
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            stage_end = time.time()
            end_time = datetime.utcnow().isoformat()
            duration = stage_end - stage_start
            
            # Parse output for metrics
            if result.stdout:
                self.logger.info(f"{stage.value} stdout:\n{result.stdout}")
                try:
                    output_json = json.loads(result.stdout)
                    stage_result.update(output_json)
                except json.JSONDecodeError:
                    pass
            
            if result.returncode != 0:
                error_msg = f"{stage.value} failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nstderr: {result.stderr}"
                raise RuntimeError(error_msg)
            
            # Create metrics for this stage
            metrics = StageMetrics(
                stage_name=stage.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                status="completed",
                row_count_input=stage_result.get("rows_input", 0),
                row_count_output=stage_result.get("rows_output", 0),
                row_count_processed=stage_result.get("rows_processed", 0),
                row_count_failed=stage_result.get("rows_failed", 0),
                memory_usage_mb=self._get_memory_usage()
            )
            
            self._log_stage_json(metrics)
            self.stage_metrics.append(metrics)
            
            self.logger.info(
                f"Stage completed: {stage.value} | "
                f"Duration: {duration:.2f}s | "
                f"Rows: input={stage_result.get('rows_input', 0)}, "
                f"output={stage_result.get('rows_output', 0)}"
            )
            
            return True, stage_result
            
        except Exception as e:
            stage_end = time.time()
            end_time = datetime.utcnow().isoformat()
            duration = stage_end - stage_start
            error_traceback = traceback.format_exc()
            
            self.logger.error(f"Stage failed: {stage.value}\n{error_traceback}")
            
            # Create metrics for failed stage
            metrics = StageMetrics(
                stage_name=stage.value,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                status="failed",
                error_message=str(e),
                error_traceback=error_traceback,
                memory_usage_mb=self._get_memory_usage()
            )
            
            self._log_stage_json(metrics)
            self.stage_metrics.append(metrics)
            
            return False, stage_result
    
    def run(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        self.logger.info(f"Pipeline execution started: {self.pipeline_id}")
        self.logger.info(f"Project root: {self.project_root}")
        
        all_successful = True
        
        try:
            # Stage 1: Load Data
            self.logger.info("=" * 80)
            self.logger.info("STAGE 1: DATA LOADING")
            self.logger.info("=" * 80)
            
            success, load_result = self._execute_stage(
                PipelineStage.DATA_LOADING,
                "load_data.py"
            )
            if not success:
                all_successful = False
                self.logger.error("Data loading stage failed. Aborting pipeline.")
                raise RuntimeError("Data loading failed")
            
            self.pipeline_state["data_loaded"] = True
            
            # Stage 2: Transform Data
            self.logger.info("\n" + "=" * 80)
            self.logger.info("STAGE 2: DATA TRANSFORMATION")
            self.logger.info("=" * 80)
            
            success, transform_result = self._execute_stage(
                PipelineStage.DATA_TRANSFORMATION,
                "transform_data.py"
            )
            if not success:
                all_successful = False
                self.logger.error("Data transformation stage failed. Aborting pipeline.")
                raise RuntimeError("Data transformation failed")
            
            self.pipeline_state["data_transformed"] = True
            
            # Stage 3: Load to Database
            self.logger.info("\n" + "=" * 80)
            self.logger.info("STAGE 3: DATABASE LOADING")
            self.logger.info("=" * 80)
            
            success, db_result = self._execute_stage(
                PipelineStage.DATABASE_LOADING,
                "load_to_db.py"
            )
            if not success:
                all_successful = False
                self.logger.error("Database loading stage failed. Pipeline partially completed.")
            
            self.pipeline_state["data_loaded_to_db"] = success
            
        except Exception as e:
            self.logger.error(f"Pipeline execution error: {e}\n{traceback.format_exc()}")
            all_successful = False
        
        finally:
            # Generate final report
            self._generate_final_report(all_successful)
        
        return all_successful
    
    def _generate_final_report(self, success: bool):
        """
        Generate comprehensive pipeline execution report.
        
        Args:
            success: Overall pipeline success status
        """
        pipeline_end = datetime.utcnow()
        total_duration = (pipeline_end - self.pipeline_start_time).total_seconds()
        
        # Calculate aggregated metrics
        total_rows_loaded = sum(m.row_count_input for m in self.stage_metrics 
                               if m.stage_name == PipelineStage.DATA_LOADING.value)
        total_rows_transformed = sum(m.row_count_output for m in self.stage_metrics 
                                    if m.stage_name == PipelineStage.DATA_TRANSFORMATION.value)
        total_rows_failed = sum(m.row_count_failed for m in self.stage_metrics)
        total_rows_db = sum(m.row_count_output for m in self.stage_metrics 
                           if m.stage_name == PipelineStage.DATABASE_LOADING.value)
        
        total_rows = total_rows_loaded + total_rows_transformed + total_rows_db
        success_rate = ((total_rows - total_rows_failed) / total_rows * 100) if total_rows > 0 else 0.0
        
        # Create pipeline metrics
        pipeline_metrics = PipelineMetrics(
            pipeline_id=self.pipeline_id,
            start_time=self.pipeline_start_time.isoformat(),
            end_time=pipeline_end.isoformat(),
            total_duration_seconds=total_duration,
            overall_status="success" if success else "failed",
            stages=[m.to_dict() for m in self.stage_metrics],
            total_rows_loaded=total_rows_loaded,
            total_rows_transformed=total_rows_transformed,
            total_rows_failed=total_rows_failed,
            total_rows_loaded_to_db=total_rows_db,
            success_rate=success_rate
        )
        
        # Write JSON report for dashboard
        report_file = self.logs_dir / f"{self.pipeline_id}_report.json"
        with open(report_file, 'w') as f:
            json.dump(pipeline_metrics.to_dict(), f, indent=2)
        
        # Write human-readable report
        summary_file = self.logs_dir / f"{self.pipeline_id}_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("ETL PIPELINE EXECUTION REPORT\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Pipeline ID: {self.pipeline_id}\n")
            f.write(f"Status: {pipeline_metrics.overall_status.upper()}\n")
            f.write(f"Start Time: {pipeline_metrics.start_time}\n")
            f.write(f"End Time: {pipeline_metrics.end_time}\n")
            f.write(f"Total Duration: {total_duration:.2f} seconds\n\n")
            
            f.write("STAGE DETAILS:\n")
            f.write("-" * 80 + "\n")
            for stage in pipeline_metrics.stages:
                f.write(f"\nStage: {stage['stage_name'].upper()}\n")
                f.write(f"  Status: {stage['status']}\n")
                f.write(f"  Duration: {stage['duration_seconds']:.2f}s\n")
                f.write(f"  Rows Input: {stage['row_count_input']}\n")
                f.write(f"  Rows Output: {stage['row_count_output']}\n")
                f.write(f"  Rows Processed: {stage['row_count_processed']}\n")
                f.write(f"  Rows Failed: {stage['row_count_failed']}\n")
                if stage['error_message']:
                    f.write(f"  Error: {stage['error_message']}\n")
            
            f.write("\n" + "-" * 80 + "\n")
            f.write("AGGREGATE METRICS:\n")
            f.write("-" * 80 + "\n")
            f.write(f"Total Rows Loaded: {pipeline_metrics.total_rows_loaded}\n")
            f.write(f"Total Rows Transformed: {pipeline_metrics.total_rows_transformed}\n")
            f.write(f"Total Rows Failed: {pipeline_metrics.total_rows_failed}\n")
            f.write(f"Total Rows Loaded to DB: {pipeline_metrics.total_rows_loaded_to_db}\n")
            f.write(f"Success Rate: {pipeline_metrics.success_rate:.2f}%\n")
            f.write("=" * 80 + "\n")
        
        # Log summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Pipeline ID: {self.pipeline_id}")
        self.logger.info(f"Status: {pipeline_metrics.overall_status.upper()}")
        self.logger.info(f"Total Duration: {total_duration:.2f} seconds")
        self.logger.info(f"Total Rows Loaded: {pipeline_metrics.total_rows_loaded}")
        self.logger.info(f"Total Rows Transformed: {pipeline_metrics.total_rows_transformed}")
        self.logger.info(f"Total Rows Failed: {pipeline_metrics.total_rows_failed}")
        self.logger.info(f"Total Rows Loaded to DB: {pipeline_metrics.total_rows_loaded_to_db}")
        self.logger.info(f"Success Rate: {pipeline_metrics.success_rate:.2f}%")
        self.logger.info("=" * 80)
        
        # Print to stdout for CI/CD integration
        print(json.dumps(pipeline_metrics.to_dict(), indent=2))


def main():
    """Main entry point for the pipeline orchestrator."""
    try:
        orchestrator = PipelineOrchestrator()
        success = orchestrator.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
