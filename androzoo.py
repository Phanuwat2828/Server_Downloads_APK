from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os
import logging
import json
from werkzeug.utils import secure_filename

# ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2 GB
app.config['MAX_FORM_MEMORY_SIZE'] = 2 * 1024 * 1024 * 1024  # 2 GB
app.config['MAX_FORM_PARTS'] = 1000

# เชื่อมต่อ MongoDB
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client.get_database()
collection = db["androzoo_task_data"]

# ตั้งค่า upload folder
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "./uploads")
ALLOWED_EXTENSIONS = {'tar.gz', 'gz', 'tar'}

# สร้าง folder ถ้ายังไม่มี
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# เชื่อมต่อ MongoDB
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client.get_database()
collection = db["androzoo_task_data"]

def allowed_file(filename):
    """ตรวจสอบนามสกุลไฟล์"""
    return filename.endswith(('.tar.gz', '.gz', '.tar'))

def create_folder_structure(sha256):
    """สร้างโครงสร้างโฟลเดอร์ตาม sha256"""
    # สร้างโฟลเดอร์ตาม sha256 แรก 2 ตัว
    folder_path = os.path.join(app.config['UPLOAD_FOLDER'])
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

@app.route('/api/upload_result', methods=['POST'])
def upload_result():
    """
    รับผลการวิเคราะห์พร้อมกันทั้งไฟล์และ report
    - file: ไฟล์ .tar.gz
    - sha256: hash ของไฟล์
    - report: JSON report (จะอยู่ใน form-data หรือเป็นไฟล์ก็ได้)
    """
    try:
        # ตรวจสอบว่าได้รับ sha256
        sha256 = request.form.get('sha256')
        if not sha256:
            return jsonify({
                "success": False,
                "error": "sha256 is required"
            }), 400
        
        # ตรวจสอบว่าได้รับ report (อาจมาจาก form-data หรือไฟล์)
        report = request.form.get('report')
        report_file = request.files.get('report_file')
        
        if not report and not report_file:
            return jsonify({
                "success": False,
                "error": "report is required (either as form-data or report_file)"
            }), 400
        
        # จัดการ report
        if report_file:
            # อ่าน report จากไฟล์
            try:
                report_content = report_file.read().decode('utf-8')
                report_json = json.loads(report_content)
            except Exception as e:
                return jsonify({
                    "success": False,
                    "error": f"Failed to read report file: {str(e)}"
                }), 400
        else:
            # report มาจาก form-data
            try:
                report_json = json.loads(report) if isinstance(report, str) else report
            except json.JSONDecodeError as e:
                return jsonify({
                    "success": False,
                    "error": f"Invalid JSON report: {str(e)}"
                }), 400
        
        # ตรวจสอบว่าได้รับไฟล์ .tar.gz
        if 'file' not in request.files:
            return jsonify({
                "success": False,
                "error": "file is required (.tar.gz)"
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                "success": False,
                "error": "no file selected"
            }), 400
        
        # ตรวจสอบนามสกุลไฟล์
        if not allowed_file(file.filename):
            return jsonify({
                "success": False,
                "error": f"file type not allowed. Allowed: {ALLOWED_EXTENSIONS}"
            }), 400
        
        # ตรวจสอบว่า document มีอยู่ใน database
        doc = collection.find_one({"sha256": sha256})
        if not doc:
            return jsonify({
                "success": False,
                "error": f"document with sha256 {sha256} not found"
            }), 404
        
        # สร้างโฟลเดอร์และ path สำหรับเก็บไฟล์
        folder_path = create_folder_structure(sha256)
        
        # สร้างชื่อไฟล์ด้วย timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = secure_filename(f"{sha256}_{timestamp}.tar.gz")
        file_path = os.path.join(folder_path, safe_filename)
        
        # บันทึกไฟล์
        file.save(file_path)
        
        # คำนวณขนาดไฟล์
        file_size = os.path.getsize(file_path)
        
        # อัปเดต document ใน database
        update_data = {
            "status": "completed",
            "path_file": file_path,
            "report": report_json,
            "time_completed": datetime.now(),
            "file_size": file_size,
            "filename": safe_filename
        }
        
        result = collection.update_one(
            {"sha256": sha256},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            return jsonify({
                "success": True,
                "message": "Result uploaded successfully",
                "data": {
                    "sha256": sha256,
                    "path_file": file_path,
                    "file_size": file_size,
                    "status": "completed"
                }
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": "Failed to update document"
            }), 500
            
    except Exception as e:
        logger.error(f"Error uploading result: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/get_next_task', methods=['GET'])
def get_next_task():
    """ดึงงานถัดไปที่ยังไม่ถูกประมวลผล (failed ก่อน แล้วค่อย waiting)"""
    try:
        # ลองดึงงานที่ failed ก่อน (เพื่อ retry)
        doc = collection.find_one_and_update(
            {
                "status": "failed"
            },
            {
                "$set": {
                    "status": "pending",
                    "time_pending": datetime.now(),
                    "retry_count": {"$inc": 1}  # เพิ่มจำนวน retry
                }
            },
            sort=[("time_update", 1)],  # เรียงตามเวลาที่อัปเดตเก่าสุดก่อน
            return_document=True
        )
        
        # ถ้าไม่มีงาน failed ให้ดึงงานที่ waiting (status != pending หรือไม่มี status)
        if not doc:
            doc = collection.find_one_and_update(
                {
                    "$or": [
                        {"status": {"$ne": "pending"}},
                        {"status": {"$exists": False}}
                    ]
                },
                {
                    "$set": {
                        "status": "pending",
                        "time_pending": datetime.now()
                    },
                    "$setOnInsert": {
                        "retry_count": 0  # ตั้งค่า retry_count ถ้ายังไม่มี
                    }
                },
                sort=[("time_update", 1)],
                return_document=True
            )
        
        if doc:
            # แปลง ObjectId เป็น string
            doc["_id"] = str(doc["_id"])
            return jsonify({
                "success": True,
                "data": doc
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": "No pending tasks available"
            }), 404
            
    except Exception as e:
        logger.error(f"Error getting next task: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500



@app.route('/api/reset_task', methods=['POST'])
def reset_task():
    """รีเซ็ตสถานะงาน (กรณีต้องการประมวลผลใหม่)"""
    try:
        data = request.json
        sha256 = data.get('sha256')
        
        if not sha256:
            return jsonify({
                "success": False,
                "error": "sha256 is required"
            }), 400
            
        result = collection.update_one(
            {"sha256": sha256},
            {"$set": {"status": "failed"}}  # หรือตั้งเป็นสถานะเดิม
        )
        
        if result.modified_count > 0:
            return jsonify({
                "success": True,
                "message": f"Task {sha256} reset to failed"
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": "Task not found"
            }), 404
            
    except Exception as e:
        logger.error(f"Error resetting task: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
    

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """ดูสถิติของงาน"""
    try:

        total = collection.count_documents({})
        pending = collection.count_documents({"status": "pending"})
        completed = collection.count_documents({"status": "completed"})
        failed = collection.count_documents({"status": "failed"})
        waiting = collection.count_documents({
            "$or": [
                {"status": {"$ne": "pending"}},
                {"status": {"$exists": False}}
            ]
        })

        vt_list = [5, 10, 15, 20, 25, 30, 35]
        vt_stats = {}
        
        for vt in vt_list:

            vt_total = collection.count_documents({"vt_detection": vt})
            vt_pending = collection.count_documents({
                "vt_detection": vt,
                "status": "pending"
            })
            vt_completed = collection.count_documents({
                "vt_detection": vt,
                "status": "completed"
            })
            vt_failed = collection.count_documents({
                "vt_detection": vt,
                "status": "failed"
            })
            vt_waiting = collection.count_documents({
                "vt_detection": vt,
                "$or": [
                    {"status": {"$ne": "pending"}},
                    {"status": {"$exists": False}}
                ]
            })
            
            vt_stats[f"vt_{vt}"] = {
                "total": vt_total,
                "waiting": vt_waiting,
                "pending": vt_pending,
                "completed": vt_completed,
                "failed": vt_failed
            }
        
        other_total = collection.count_documents({
            "vt_detection": {"$nin": vt_list}
        })
        other_waiting = collection.count_documents({
            "vt_detection": {"$nin": vt_list},
            "$or": [
                {"status": {"$ne": "pending"}},
                {"status": {"$exists": False}}
            ]
        })
        other_pending = collection.count_documents({
            "vt_detection": {"$nin": vt_list},
            "status": "pending"
        })
        other_completed = collection.count_documents({
            "vt_detection": {"$nin": vt_list},
            "status": "completed"
        })
        other_failed = collection.count_documents({
            "vt_detection": {"$nin": vt_list},
            "status": "failed"
        })
        
        vt_stats["other"] = {
            "total": other_total,
            "waiting": other_waiting,
            "pending": other_pending,
            "completed": other_completed,
            "failed": other_failed
        }
        
        return jsonify({
            "success": True,
            "stats": {
                "total": total,
                "waiting": waiting,
                "pending": pending,
                "completed": completed,
                "failed": failed,
                "by_vt_detection": vt_stats
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/get_report/<sha256>', methods=['GET'])
def get_report(sha256):
    """
    ดึง report ของไฟล์ตาม sha256
    Query params:
    - format: 'json' (default) หรือ 'pretty' (แบบสวยงาม)
    """
    try:
        # ดึงข้อมูลจาก database
        doc = collection.find_one({"sha256": sha256})
        
        if not doc:
            return jsonify({
                "success": False,
                "error": f"Document with sha256 {sha256} not found"
            }), 404
        
        # ดึง report
        report = doc.get("report", {})
        
        # ตรวจสอบว่ามี report หรือไม่
        if not report:
            return jsonify({
                "success": False,
                "error": "No report available for this file",
                "data": {
                    "sha256": sha256,
                    "status": doc.get("status"),
                    "has_report": False
                }
            }), 404
        
        # รูปแบบการแสดงผล
        format_type = request.args.get('format', 'json')
        
        if format_type == 'pretty':
            # ส่งกลับเป็น HTML แบบสวยงาม
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Report for {sha256[:15]}...</title>
                <style>
                    body {{ font-family: monospace; margin: 20px; background: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
                    h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
                    h2 {{ color: #555; margin-top: 20px; }}
                    pre {{ background: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; }}
                    .info {{ background: #e3f2fd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                    .status {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
                    .completed {{ background: #4CAF50; color: white; }}
                    .failed {{ background: #f44336; color: white; }}
                    .pending {{ background: #ff9800; color: white; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>📊 Analysis Report</h1>
                    <div class="info">
                        <strong>SHA256:</strong> {sha256}<br>
                        <strong>Status:</strong> <span class="status {doc.get('status', 'unknown')}">{doc.get('status', 'unknown')}</span><br>
                        <strong>Package:</strong> {doc.get('pkg_name', 'N/A')}<br>
                        <strong>VT Detection:</strong> {doc.get('vt_detection', 'N/A')}<br>
                        <strong>Analysis Time:</strong> {doc.get('time_completed', doc.get('time_update', 'N/A'))}<br>
                        <strong>File Size:</strong> {doc.get('file_size', 0):,} bytes
                    </div>
                    <h2>📄 Report Content</h2>
                    <pre>{json.dumps(report, indent=2, ensure_ascii=False)}</pre>
                </div>
            </body>
            </html>
            """
            return html_content, 200, {'Content-Type': 'text/html'}
        else:
            # ส่งกลับเป็น JSON
            return jsonify({
                "success": True,
                "data": {
                    "sha256": sha256,
                    "status": doc.get("status"),
                    "pkg_name": doc.get("pkg_name"),
                    "vt_detection": doc.get("vt_detection"),
                    "time_completed": doc.get("time_completed"),
                    "file_size": doc.get("file_size"),
                    "report": report
                }
            }), 200
            
    except Exception as e:
        logger.error(f"Error getting report: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)