import fpdf
from datetime import datetime
import qrcode
import os

class CareerProofPDF(fpdf.FPDF):
    def header(self):
        # ლურჯი ჰედერი
        self.set_fill_color(30, 41, 59)
        self.rect(0, 0, 210, 40, 'F')
        self.set_font('Arial', 'B', 22)
        self.set_text_color(255, 255, 255)
        self.cell(0, 20, 'DISTRO-SMART SYSTEM', 0, 1, 'C')
        self.ln(15)

    def footer(self):
        self.set_y(-30)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, 'დადასტურებულია Distro-Smart სისტემით | შეუცვლელი საოპერაციო ჩანაწერები', 0, 1, 'C')

def generate_report(user_data):
    pdf = CareerProofPDF()
    pdf.add_page()
    
    # მომხმარებლის ინფორმაცია
    pdf.set_font('Arial', 'B', 16)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(0, 10, f"თანამშრომელი: {user_data['name']}", 0, 1)
    
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 7, f"პოზიცია: {user_data.get('role', 'სპეციალისტი')}", 0, 1)
    pdf.cell(0, 7, f"სიზუსტე: {user_data.get('accuracy', '99.0')}%", 0, 1)
    pdf.ln(10)

    # QR კოდის შექმნა
    verify_url = f"https://distro-smart.com/verify/{user_data['name'].replace(' ', '_')}"
    qr = qrcode.QRCode(box_size=10, border=4)
    qr.add_data(verify_url)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")
    qr_path = "temp_qr.png"
    qr_img.save(qr_path)

    # QR კოდის ჩასმა PDF-ში
    pdf.image(qr_path, x=165, y=240, w=30)
    pdf.set_y(270)
    pdf.set_x(165)
    pdf.set_font('Arial', 'I', 7)
    pdf.cell(30, 5, "დადასტურების QR", 0, 0, 'C')

    # შენახვა
    filename = f"Career_Proof_{user_data['name'].replace(' ', '_')}.pdf"
    pdf.output(filename)
    
    if os.path.exists(qr_path):
        os.remove(qr_path)
        
    return filename