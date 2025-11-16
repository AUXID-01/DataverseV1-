# app/routes/records_router.py

from fastapi import APIRouter, Query, HTTPException
from app.database import db
from app.utils.mongo import clean_mongo_document

router = APIRouter(prefix="/records", tags=["Records"])


@router.get("/")
async def get_records(
    source_id: str = Query(..., description="Source ID of uploaded file"),
    limit: int = Query(200, ge=1, le=2000, description="Max number of records to return")
):
    """
    Fetch cleaned + transformed records for a given source_id.
    This uses the `records` collection filled during ETL upload.
    """

    cursor = (
        db.records
        .find({"source_id": source_id})
        .sort("uploaded_at", -1)
        .limit(limit)
    )

    results = []
    async for doc in cursor:
        results.append(clean_mongo_document(doc))

    if not results:
        raise HTTPException(status_code=404, detail="No records found for this source_id")

    return {
        "source_id": source_id,
        "count": len(results),
        "records": results,
    }


@router.get("/{record_id}")
async def get_single_record(record_id: str):
    """
    Fetch a single cleaned record by Mongo _id.
    """

    from bson import ObjectId

    try:
        oid = ObjectId(record_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid record_id format")

    doc = await db.records.find_one({"_id": oid})

    if not doc:
        raise HTTPException(status_code=404, detail="Record not found")

    return clean_mongo_document(doc)
