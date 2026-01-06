import json
import os

def setup_storage():
    """
    LOGIC ADMIN: Sinkronisasi users.json dengan folder fisik di storage.
    """
    json_path = "users.json"
    storage_base = "storage"

    print("="*45)
    print("🚀 [ADMIN] STORAGE & TENANT INITIALIZER")
    print("="*45)

    # 1. Cek apakah users.json ada
    if not os.path.exists(json_path):
        print(f"❌ Error: File '{json_path}' tidak ditemukan!")
        return

    # 2. Baca data user dari JSON
    with open(json_path, "r") as f:
        users = json.load(f)

    # 3. Iterasi setiap perusahaan di dalam JSON
    for client_id, info in users.items():
        print(f"\n🔎 Mengecek Tenant: {client_id} ({info.get('company_name')})")
        
        # Tentukan path folder utama PT
        client_dir = os.path.join(storage_base, client_id)
        
        # Daftar sub-folder yang wajib ada
        sub_folders = ["Raw", "Clean", "Downloads"]

        if not os.path.exists(client_dir):
            print(f"  ✨ Perusahaan Baru Terdeteksi! Membuat folder utama...")
            os.makedirs(client_dir, exist_ok=True)
        else:
            print(f"  ✅ Folder utama sudah ada, mengecek sub-folder...")

        # 4. Buat sub-folder Raw, Clean, Downloads
        for sub in sub_folders:
            path = os.path.join(client_dir, sub)
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
                print(f"    + Berhasil membuat folder: {sub}")
            else:
                print(f"    - Folder {sub} sudah tersedia.")

    print("\n" + "="*45)
    print("✅ Selesai! Semua folder sudah sinkron dengan users.json.")
    print("="*45)

if __name__ == "__main__":
    setup_storage()