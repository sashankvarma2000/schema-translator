"""Storage adapters for local filesystem and cloud storage."""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml

from ..core.config import settings
from ..shared.logging import logger
from ..shared.models import MappingPlan


class LocalStorageAdapter:
    """Local filesystem storage adapter."""
    
    def __init__(self, base_path: Optional[Path] = None):
        self.base_path = base_path or settings.output_dir
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_mapping_plan(self, tenant: str, mapping_plan: MappingPlan) -> str:
        """Save a mapping plan to local storage."""
        filename = f"{tenant}_mapping_plan_v{mapping_plan.version}.json"
        file_path = self.base_path / "mapping_plans" / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w') as f:
            json.dump(mapping_plan.dict(), f, indent=2, default=str)
        
        logger.info(f"Saved mapping plan to {file_path}")
        return str(file_path)
    
    def load_mapping_plan(self, tenant: str, version: str = "latest") -> Optional[MappingPlan]:
        """Load a mapping plan from local storage."""
        plans_dir = self.base_path / "mapping_plans"
        
        if version == "latest":
            # Find the latest version
            pattern = f"{tenant}_mapping_plan_v*.json"
            plan_files = list(plans_dir.glob(pattern))
            if not plan_files:
                return None
            # Sort by modification time, get most recent
            file_path = max(plan_files, key=lambda p: p.stat().st_mtime)
        else:
            filename = f"{tenant}_mapping_plan_v{version}.json"
            file_path = plans_dir / filename
            if not file_path.exists():
                return None
        
        with open(file_path, 'r') as f:
            plan_data = json.load(f)
        
        return MappingPlan(**plan_data)
    
    def save_transformed_data(
        self, 
        tenant: str, 
        data: pd.DataFrame, 
        version: str,
        format: str = "parquet"
    ) -> str:
        """Save transformed data."""
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        
        if format == "parquet":
            filename = f"{tenant}_canonical_v{version}_{timestamp}.parquet"
            file_path = self.base_path / "transformed_data" / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            data.to_parquet(file_path, index=False)
        elif format == "csv":
            filename = f"{tenant}_canonical_v{version}_{timestamp}.csv"
            file_path = self.base_path / "transformed_data" / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Saved transformed data to {file_path}")
        return str(file_path)
    
    def save_lineage_records(self, tenant: str, lineage_data: Dict[str, Any]) -> str:
        """Save lineage information."""
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{tenant}_lineage_{timestamp}.json"
        file_path = self.base_path / "lineage" / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w') as f:
            json.dump(lineage_data, f, indent=2, default=str)
        
        logger.info(f"Saved lineage records to {file_path}")
        return str(file_path)


class S3StorageAdapter:
    """AWS S3 storage adapter (placeholder for future implementation)."""
    
    def __init__(self, bucket_name: str, prefix: str = "schema-translator"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        # TODO: Initialize boto3 client
        raise NotImplementedError("S3 adapter not yet implemented")
    
    def save_mapping_plan(self, tenant: str, mapping_plan: MappingPlan) -> str:
        """Save mapping plan to S3."""
        raise NotImplementedError("S3 adapter not yet implemented")
    
    def load_mapping_plan(self, tenant: str, version: str = "latest") -> Optional[MappingPlan]:
        """Load mapping plan from S3."""
        raise NotImplementedError("S3 adapter not yet implemented")
