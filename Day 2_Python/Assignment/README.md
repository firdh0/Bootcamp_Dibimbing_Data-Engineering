# 📦 DAY 2 - ASSIGNMENT

## 🚀 Project Overview

This project focuses on Python programming tasks, specifically generating PDFs and working with various assets like fonts and logos. The assignments are part of the Bootcamp Dibimbing Data Engineering program.

## 📁 Project Structure

The project is organized as follows:

```
.
├── 📄 PDF Generator.ipynb      # 📊 Jupyter Notebook for PDF generation
├── Fonts/                       # 🔤 Font files used in the PDF generation process
├── Hasil/                       # 📂 Directory for storing generated PDF files
├── Logo/                        # 🖼 Logo images for inclusion in PDFs
├── README.md                    # 📘 Project documentation (this file)
```

## ✨ Features

* 📄 **PDF Generator**: A Jupyter Notebook that demonstrates how to generate PDF files programmatically using Python.
* 🔤 **Fonts**: Includes font files required for customizing the appearance of the PDFs.
* 🖼 **Logos**: Contains logo images that can be embedded in the generated PDFs.

## ✅ Prerequisites

* 🐍 Python 3.x
* 📓 Jupyter Notebook or JupyterLab

## 🛠️ Usage

1. Open the `📄 PDF Generator.ipynb` notebook in Jupyter Notebook or JupyterLab.
2. Install the required Python packages by running the following commands in a code cell:

   ```bash
   pip install langchain-community reportlab python-dotenv
   pip install -U langchain-openai
   ```

3. Follow the instructions in the notebook to generate PDF files.
4. Ensure that the required fonts and logos are placed in the `Fonts/` and `Logo/` directories, respectively.
5. The generated PDFs will be saved in the `Hasil/` directory.
6. **API Requirement**: Ensure you have access to the GPT API and set up the necessary API keys in your environment to enable advanced text generation features.

## 🗒️ Notes

* Ensure that all necessary assets (fonts and logos) are available in their respective directories before running the notebook.
* Customize the notebook as needed to fit specific requirements.