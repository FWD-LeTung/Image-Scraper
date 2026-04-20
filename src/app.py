import customtkinter as ctk
from tkinter import filedialog, messagebox
import threading
import asyncio
import random
import sys
import os
import subprocess
from collections import Counter
import polars as pl
from playwright.async_api import async_playwright
from playwright_stealth import Stealth


ctk.set_appearance_mode("System")  
ctk.set_default_color_theme("blue")

MAX_CONCURRENT_TABS = 3
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"


class HarvesterApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("The Stealthy Image Harvester - Pro Edition")
        self.geometry("650x480")
        self.resizable(False, False)

        self.input_file_path = ""
        self.output_file_path = ""
        self.is_running = False

        self.build_ui()

    def build_ui(self):
        # 1. Tiêu đề
        self.title_label = ctk.CTkLabel(self, text="IMAGE HARVESTER", font=ctk.CTkFont(size=26, weight="bold"))
        self.title_label.pack(pady=(20, 5))

        self.subtitle_label = ctk.CTkLabel(self, text="Scraper", text_color="gray")
        self.subtitle_label.pack(pady=(0, 20))

        # 2. Khu vực chọn file
        self.file_frame = ctk.CTkFrame(self)
        self.file_frame.pack(pady=10, padx=30, fill="x")

        self.file_label = ctk.CTkLabel(self.file_frame, text="Chưa chọn file Excel/CSV nào...", width=350, anchor="w")
        self.file_label.pack(side="left", padx=15, pady=15)

        self.btn_select_file = ctk.CTkButton(self.file_frame, text="Chọn File", command=self.select_file, width=100)
        self.btn_select_file.pack(side="right", padx=15)

        # 3. Tùy chọn Chế độ
        self.mode_var = ctk.BooleanVar(value=True) 
        self.switch_mode = ctk.CTkSwitch(
            self, text="Deep Scan (Chỉ chọn khi lấy ảnh từ link trang chủ)", 
            variable=self.mode_var, font=ctk.CTkFont(weight="bold")
        )
        self.switch_mode.pack(pady=15)

        # 4. Thanh tiến trình
        self.progress_label = ctk.CTkLabel(self, text="Trạng thái: Sẵn sàng", font=ctk.CTkFont(size=13))
        self.progress_label.pack(pady=(15, 5))

        self.progress_bar = ctk.CTkProgressBar(self, width=500)
        self.progress_bar.pack(pady=0)
        self.progress_bar.set(0)

        # 5. Nút Bắt đầu
        self.btn_start = ctk.CTkButton(
            self, text=" BẮT ĐẦU THU HOẠCH", height=45, 
            font=ctk.CTkFont(size=14, weight="bold"), command=self.start_scraping
        )
        self.btn_start.pack(pady=25)

    def select_file(self):
        file_path = filedialog.askopenfilename(
            title="Chọn file dữ liệu",
            filetypes=(("Excel files", "*.xlsx"), ("CSV files", "*.csv"), ("All files", "*.*"))
        )
        if file_path:
            self.input_file_path = file_path
            # Tự động tạo tên file kết quả cùng thư mục
            self.output_file_path = file_path.rsplit('.', 1)[0] + "_KET_QUA.xlsx"
            display_name = file_path.split("/")[-1]
            self.file_label.configure(text=f"📂 Đã chọn: {display_name}", text_color="#28a745")

    def sync_log(self, current, total, message):
        """Hàm đồng bộ (Callback) để luồng phụ cập nhật UI một cách an toàn"""
        def update():
            if total > 0:
                self.progress_bar.set(current / total)
            self.progress_label.configure(text=message)
        self.after(0, update)

    def finish_ui(self, success=True, msg=""):
        """Khôi phục UI sau khi chạy xong"""
        self.is_running = False
        self.btn_start.configure(state="normal", text="BẮT ĐẦU THU HOẠCH")
        self.btn_select_file.configure(state="normal")
        self.switch_mode.configure(state="normal")
        
        if success:
            self.progress_bar.set(1)
            self.progress_label.configure(text="Hoàn tất thành công!", text_color="#28a745")
            messagebox.showinfo("Thành công", f"Đã quét xong!\nFile lưu tại:\n{self.output_file_path}")
        else:
            self.progress_bar.set(0)
            self.progress_label.configure(text="Có lỗi xảy ra!", text_color="#dc3545")
            messagebox.showerror("Lỗi", msg)

    def start_scraping(self):
        if not self.input_file_path:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn file dữ liệu đầu vào!")
            return
        if self.is_running:
            return

        self.is_running = True
        self.btn_start.configure(state="disabled", text="⏳ ĐANG CHẠY...")
        self.btn_select_file.configure(state="disabled")
        self.switch_mode.configure(state="disabled")
        self.progress_bar.set(0)

        is_deep_scan = self.mode_var.get()

        # Khởi chạy Worker Thread để không làm đơ giao diện
        threading.Thread(target=self.worker_thread, args=(is_deep_scan,), daemon=True).start()

    # ================= KHU VỰC CHẠY NỀN (WORKER THREAD) =================
    def worker_thread(self, is_deep_scan):
        try:
            # 1. Cài đặt tự động môi trường Firefox ngầm (Cho lần chạy đầu tiên)
            self.sync_log(0, 1, "Đang kiểm tra & khởi tạo trình duyệt ẩn danh...")
            subprocess.run(["playwright", "install", "firefox"], shell=True, capture_output=True)

            # 2. Cài đặt Event Loop cho Asyncio trên Thread phụ
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.async_scraper(is_deep_scan))
            loop.close()

        except Exception as e:
            self.after(0, self.finish_ui, False, f"Lỗi hệ thống:\n{str(e)}")

    # ================= KHU VỰC LÕI CÀO DỮ LIỆU (ASYNCIO) =================
    async def get_images_from_page(self, page, url):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await asyncio.sleep(random.uniform(0.5, 1.2))
            await page.wait_for_load_state("networkidle", timeout=10000)
            
            img_links = await page.locator("img").evaluate_all(
                """imgs => imgs.map(img => img.src || img.getAttribute('data-src') || img.srcset)
                           .filter(src => src && src.startsWith('http'))"""
            )
            return list(set(img_links))
        except:
            return []

    async def crawl_internal_links(self, page, root_url):
        try:
            await page.goto(root_url, wait_until="networkidle", timeout=30000)
            links = await page.locator("a").evaluate_all(
                f"elements => elements.map(a => a.href).filter(href => href.includes('{root_url}') && !href.includes('#'))"
            )
            return list(set(links))
        except:
            return []

    async def process_task(self, browser_context, url, semaphore, is_deep_scan):
        async with semaphore:
            main_page = await browser_context.new_page()
            all_images = []
            
            if is_deep_scan:
                sub_links = await self.crawl_internal_links(main_page, url)
                filtered_links = [link for link in sub_links if len(link) > 25]
                await main_page.close()

                sub_semaphore = asyncio.Semaphore(3) 
                
                async def fetch_sub_page(sub_url):
                    async with sub_semaphore:
                        sub_page = await browser_context.new_page()
                        imgs = await self.get_images_from_page(sub_page, sub_url)
                        await sub_page.close()
                        return list(set(imgs))

                sub_tasks = [fetch_sub_page(sub_url) for sub_url in filtered_links]
                sub_results = await asyncio.gather(*sub_tasks)
                
                # Thuật toán lọc Logo/Banner
                all_images_flat = []
                for img_list in sub_results:
                    all_images_flat.extend(img_list)
                    
                img_counts = Counter(all_images_flat)
                unique_images = [img for img, count in img_counts.items() if count == 1]
                all_images.extend(unique_images)
            else:
                imgs = await self.get_images_from_page(main_page, url)
                all_images.extend(imgs)
                await main_page.close()
                
            return {"Nguồn": url, "Ảnh": list(set(all_images))}

    async def async_scraper(self, is_deep_scan):
        # 1. Đọc File
        self.sync_log(0, 1, "Đang đọc dữ liệu từ file...")
        if self.input_file_path.endswith('.csv'):
            df = pl.read_csv(self.input_file_path, infer_schema_length=0)
        else:
            df = pl.read_excel(self.input_file_path, engine="openpyxl")

        # Lấy cột URL (Ưu tiên cột thứ 2 [index 1], nếu file chỉ có 1 cột thì lấy cột 1)
        url_col_idx = 1 if len(df.columns) > 1 else 0
        urls = df.get_column(df.columns[url_col_idx]).drop_nulls().cast(pl.String).to_list()
        total_urls = len(urls)

        # 2. Khởi động Playwright
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        results = []
        completed = 0

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context(user_agent=USER_AGENT)
            
            # Khởi tạo danh sách tác vụ
            tasks = [self.process_task(context, url, semaphore, is_deep_scan) for url in urls]
            
            self.sync_log(0, total_urls, f"Bắt đầu xử lý {total_urls} URLs...")
            
            # Cập nhật UI ngay khi mỗi link gốc chạy xong
            for coro in asyncio.as_completed(tasks):
                res = await coro
                results.append(res)
                completed += 1
                # Lấy 30 ký tự đầu của URL để hiển thị cho gọn
                short_url = res["Nguồn"][:30] + "..." if len(res["Nguồn"]) > 30 else res["Nguồn"]
                self.sync_log(completed, total_urls, f"Đã quét: {short_url} ({completed}/{total_urls})")

            await browser.close()

        # 3. Ghi File Bằng Polars
        self.sync_log(total_urls, total_urls, "Đang trích xuất và lưu file Excel...")
        max_images = max((len(res["Ảnh"]) for res in results), default=0)
        data_dict = {"URL_Gốc": [res["Nguồn"] for res in results]}
        
        for i in range(max_images):
            data_dict[f"Link_Ảnh_{i+1}"] = [
                res["Ảnh"][i] if i < len(res["Ảnh"]) else None for res in results
            ]
            
        final_df = pl.DataFrame(data_dict)
        final_df.write_excel(self.output_file_path)

        # Báo cáo kết quả
        self.after(0, self.finish_ui, True, "")

if __name__ == "__main__":
    app = HarvesterApp()
    app.mainloop()