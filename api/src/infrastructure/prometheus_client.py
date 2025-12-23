import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

import httpx

from config import Config


logger = logging.getLogger(__name__)


class PrometheusClient:
    def __init__(self, base_url: str = None):
        self._base_url = base_url or Config.PROMETHEUS_URL

    async def query(self, query: str) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self._base_url}/api/v1/query",
                    params={"query": query},
                    timeout=10.0,
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") != "success":
                    logger.error("Prometheus query failed: %s", data)
                    return []
                
                return data.get("data", {}).get("result", [])
            except httpx.HTTPError as e:
                logger.error("Prometheus request failed: %s", e)
                return []

    async def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step: str = "1m",
    ) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self._base_url}/api/v1/query_range",
                    params={
                        "query": query,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "step": step,
                    },
                    timeout=10.0,
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") != "success":
                    logger.error("Prometheus range query failed: %s", data)
                    return []
                
                return data.get("data", {}).get("result", [])
            except httpx.HTTPError as e:
                logger.error("Prometheus request failed: %s", e)
                return []


async def parse_prometheus_metrics(url: str) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=5.0)
            response.raise_for_status()
            return _parse_prometheus_text(response.text)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch metrics from %s: %s", url, e)
            return {"error": str(e)}


def _parse_prometheus_text(text: str) -> Dict[str, Any]:
    metrics = {}
    current_metric = None
    current_help = None
    current_type = None
    
    for line in text.strip().split("\n"):
        line = line.strip()
        
        if not line:
            continue
        
        if line.startswith("# HELP"):
            parts = line.split(" ", 3)
            if len(parts) >= 4:
                current_metric = parts[2]
                current_help = parts[3]
        elif line.startswith("# TYPE"):
            parts = line.split(" ", 3)
            if len(parts) >= 4:
                current_type = parts[3]
        elif not line.startswith("#"):
            metric_data = _parse_metric_line(line)
            if metric_data:
                name = metric_data["name"]
                if name not in metrics:
                    metrics[name] = {
                        "help": current_help,
                        "type": current_type,
                        "values": [],
                    }
                metrics[name]["values"].append({
                    "labels": metric_data["labels"],
                    "value": metric_data["value"],
                })
    
    return metrics


def _parse_metric_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        if "{" in line:
            name_end = line.index("{")
            name = line[:name_end]
            labels_end = line.index("}")
            labels_str = line[name_end + 1:labels_end]
            value_str = line[labels_end + 1:].strip()
            
            labels = {}
            if labels_str:
                for pair in labels_str.split(","):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        labels[k.strip()] = v.strip().strip('"')
            
            return {
                "name": name,
                "labels": labels,
                "value": float(value_str),
            }
        else:
            parts = line.split()
            if len(parts) >= 2:
                return {
                    "name": parts[0],
                    "labels": {},
                    "value": float(parts[1]),
                }
    except (ValueError, IndexError) as e:
        logger.debug("Failed to parse metric line: %s - %s", line, e)
    
    return None

