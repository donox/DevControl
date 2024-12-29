import json
import logging

from pdf2image import convert_from_path
import pytesseract
import pandas as pd
import os
import svgwrite
import tempfile
from utils import temp_file_rw as temp_mgr

def extract_text_from_image(images, output_dir=None):
    """
    Extracts text and bounding boxes from images using Tesseract.

    Args:
        images (list): Images representing sections or pages of a 990PF
        output_dir (str): Directory to save the extracted images and TSV files. If None, uses the current directory.

    Returns:
        str: Path to the temporary file containing the OCR results.
    """
    if output_dir is None:
        output_dir = os.getcwd() + "/pdf_images"

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # # Convert PDF to images
    # print("Converting PDF to images...")
    # images = convert_from_path(pdf_path, dpi=300)

    results = {}

    for page_number, image in enumerate(images, start=0):
        # Save image to output directory
        image_path = os.path.join(output_dir, f"page_{page_number}.png")
        image.save(image_path, "PNG")

        # Perform OCR with Tesseract to extract text and bounding boxes
        logging.info(f"Processing page {page_number}...")
        ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DATAFRAME)

        # Filter out empty rows
        ocr_data = ocr_data[ocr_data["text"].notnull() & (ocr_data["text"].str.strip() != "")]

        # Store results in dictionary
        results[page_number] = ocr_data.to_dict()

    temp_file_path = temp_mgr.write_to_temp_file(results)
    logging.info(f"process_pdf complete")
    return temp_file_path

def identify_form_elements(temp_file_path):
    """
    Analyzes OCR results to find form elements (titles and values) based on spatial consistency.

    Args:
        temp_file_path (str): Path to the temporary file containing OCR results.

    Returns:
        str: Path to the temporary file containing identified form elements.
    """
    ocr_data = temp_mgr.read_from_temp_file(temp_file_path)
    outpath = "/home/don/Documents/Temp/WW990/structure/page_dfs/page"
    # ocr_data = temp_file_path
    form_elements = []

    for page_number, page_data in ocr_data.items():
        page_df = pd.DataFrame(page_data)

        # Group by block and line to identify clusters of text
        grouped = page_df.groupby(['block_num', 'line_num'])

        for (block_num, line_num), group in grouped:
            group = group.sort_values(by='left')  # Sort text by horizontal position

            # Concatenate text in the line to form potential titles or labels
            line_text = " ".join(group['text'].tolist())

            # Calculate the bounding box for the entire line
            x_min = group['left'].min()
            y_min = group['top'].min()
            x_max = (group['left'] + group['width']).max()
            y_max = (group['top'] + group['height']).max()

            # Store the line as a potential form element
            form_elements.append({
                'text': line_text,
                'bounding_box': {
                    'x_min': x_min,
                    'y_min': y_min,
                    'x_max': x_max,
                    'y_max': y_max
                },
                'page': page_number
            })
            of = outpath + str(page_number) + ".csv"
            page_df.to_csv(of, index=True)

    return temp_mgr.write_to_temp_file(form_elements)

def create_svg_from_containers(temp_file_path):
    temp_file_path = "/home/don/Documents/Temp/WW990/structure/box_structure.json"
    output_file = "/home/don/Documents/Temp/WW990/structure/draw.svg"
    """
    Creates an SVG drawing for a list of bounding boxes, numbering them in the upper-left corner.

    Args:
        temp_file_path (str): Path to the temporary file containing form elements.
        output_file (str): Path to save the SVG file.
    """
    containers_all = temp_mgr.read_from_temp_file(temp_file_path)
    containers = [x for x in containers_all if x['page']=="0"]
    # Create an SVG drawing
    dwg = svgwrite.Drawing(output_file, profile='tiny')
    # Add white background rectangle
    dwg.add(dwg.rect(insert=(0, 0), size=('100%', '100%'), fill='white'))

    for idx, container in enumerate(containers, start=0):
        bbox = container['bounding_box']
        x_min, y_min = bbox['x_min'], bbox['y_min']
        x_max, y_max = bbox['x_max'], bbox['y_max']
        x_text = container['text'][:10]
        # Draw the rectangle
        dwg.add(dwg.rect(insert=(x_min, y_min), size=(x_max - x_min, y_max - y_min),
                         stroke='black', fill='none', stroke_width=5))

        # Add the number in the upper-left corner
        dwg.add(dwg.text(str(idx), insert=(x_min + 2, y_min + 12), fill='red', font_size='16px',
                         font_weight='bold'))
        # Add text in the upper-left
        dwg.add(dwg.text(str(x_text), insert=(x_min + 14, y_min + 18), fill='black', font_size='12px'))

    # Save the SVG
    dwg.save()
    print(f"SVG saved to {output_file}")

# Example usage
if __name__ == "__main__":
    pdf_path = "example.pdf"  # Replace with your PDF file path
    output_dir = "output"  # Replace with your desired output directory

    # Step 1: Process PDF
    ocr_results_file = process_pdf(pdf_path, output_dir)

    # Step 2: Identify form elements
    form_elements_file = identify_form_elements(ocr_results_file)

    # Step 3: Create SVG
    svg_output = os.path.join(output_dir, "form_elements.svg")
    create_svg_from_containers(form_elements_file, svg_output)
