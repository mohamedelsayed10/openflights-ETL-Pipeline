import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from create_schema import create
from extract import extract_data
from transform import transform_data
from load import load_data

# GUI Setup
class ETLApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL Pipeline GUI")
        self.root.geometry("400x450")
        self.root.config(bg="#2c3e50")  # Dark background color

        # Create Stylish Components
        self.header_label = tk.Label(root, text="ETL Pipeline", font=("Helvetica", 18, 'bold'), fg="#ecf0f1", bg="#2c3e50")
        self.header_label.pack(pady=20)

        # Button for each step of the ETL process
        self.create_button = self.create_styled_button(root, "Create Schema", self.run_create)
        self.create_button.pack(pady=10)


        self.extract_button = self.create_styled_button(root, "Extract Data", self.run_extract)
        self.extract_button.pack(pady=10)

        self.transform_button = self.create_styled_button(root, "Transform Data", self.run_transform)
        self.transform_button.pack(pady=10)

        self.load_button = self.create_styled_button(root, "Load Data", self.run_load)
        self.load_button.pack(pady=10)

        # Button for running all ETL steps
        self.etl_button = self.create_styled_button(root, "Run Full ETL", self.run_full_etl)
        self.etl_button.pack(pady=10)

        # Exit button to close the application
        self.exit_button = self.create_styled_button(root, "Exit", root.destroy)
        self.exit_button.pack(pady=10)

        # Status label to show progress
        self.status_label = tk.Label(root, text="ETL Status: Waiting", font=("Arial", 12), fg="#ecf0f1", bg="#34495e")
        self.status_label.pack(pady=20, padx=10, fill='x')

    def create_styled_button(self, root, text, command):
        return tk.Button(root, text=text, font=("Arial", 12), command=command, bg="#3498db", fg="white", relief="flat", width=20)

    def update_status(self, message):
        self.status_label.config(text=f"ETL Status: {message}")

    def run_create(self):
        self.update_status("Creating Schema...")
        try:
            create()
            self.update_status("Schema Created")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in creating schema: {e}")

    def run_raw_data_to_db(self):
        self.update_status("Loading Raw Data...")
        try:
            inset_raw_data_to_db()
            self.update_status("Raw Data Loaded")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in loading raw data: {e}")

    def run_extract(self):
        self.update_status("Extracting Data...")
        try:
            self.airports, self.airlines, self.routes = extract_data()
            self.update_status("Data Extracted")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in extracting data: {e}")

    def run_transform(self):
        self.update_status("Transforming Data...")
        try:
            self.airports, self.airlines, self.routes = transform_data(self.airports, self.airlines, self.routes)
            self.update_status("Data Transformed")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in transforming data: {e}")

    def run_load(self):
        self.update_status("Loading Data...")
        try:
            load_data(self.airports, self.airlines, self.routes)
            self.update_status("Data Loaded Successfully")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in loading data: {e}")

    def run_full_etl(self):
        self.update_status("Running Full ETL...")
        try:
            create()  # Create schema
            self.airports, self.airlines, self.routes = extract_data()  # Extract data
            self.airports, self.airlines, self.routes = transform_data(self.airports, self.airlines, self.routes)  # Transform data
            load_data(self.airports, self.airlines, self.routes)  # Load data
            self.update_status("Full ETL Completed Successfully")
            messagebox.showinfo("Success", "ETL Process Completed Successfully!")
        except Exception as e:
            self.update_status(f"Error: {e}")
            messagebox.showerror("Error", f"Error in Full ETL process: {e}")

# Main
def main():
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
