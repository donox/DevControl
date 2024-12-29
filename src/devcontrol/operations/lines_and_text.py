import csv
import os
import pytesseract
import numpy as np
import cv2

from PIL import Image
import pdf2image


def extract_pdf_pages(pdf_path, output_dir):
    """Extract pages from PDF to TIFF files"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Convert PDF pages to images
    pages = pdf2image.convert_from_path(pdf_path)[0:1]

    # Save each page as TIFF
    tiff_files = []
    for i, page in enumerate(pages):
        tiff_path = os.path.join(output_dir, f'page_{i + 1}.tiff')
        page.save(tiff_path, 'TIFF')
        tiff_files.append(tiff_path)

    return tiff_files


def preprocess_image(image_path):
    img = cv2.imread(image_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Binary threshold to separate lines from text
    _, binary = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)

    # Separate horizontal and vertical elements
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 1))
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 25))

    # Detect horizontal lines
    horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel)

    # Detect vertical lines
    vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel)

    # Combine horizontal and vertical lines
    lines = cv2.bitwise_or(horizontal, vertical)

    return lines, img


def detect_lines_and_boxes(image_path):
    edges, original = preprocess_image(image_path)

    # Detect lines (this part stays the same)
    lines = cv2.HoughLinesP(
        edges,
        rho=1,
        theta=np.pi / 180,
        threshold=50,
        minLineLength=100,
        maxLineGap=10
    )

    # Separate into horizontal and vertical lines
    horizontal_lines = []
    vertical_lines = []

    # Track statistics (your existing statistics code stays here)
    if lines is not None:
        for line in lines:
            x1, y1, x2, y2 = line[0]
            dx = x2 - x1
            dy = y2 - y1

            if abs(dy) < 5:  # Horizontal
                horizontal_lines.append([x1, y1, x2, y2])
            elif abs(dx) < 5:  # Vertical
                vertical_lines.append([x1, y1, x2, y2])

    # Add form boundaries if missing
    vertical_lines = ensure_form_boundaries(vertical_lines, horizontal_lines, original.shape)

    # Merge nearby lines
    horizontal_lines = merge_nearby_lines(horizontal_lines)
    vertical_lines = merge_nearby_lines(vertical_lines)

    # Draw lines for debugging (your existing visualization code)
    debug_img = original.copy()
    for line in horizontal_lines:
        x1, y1, x2, y2 = map(int, line)
        cv2.line(debug_img, (x1, y1), (x2, y2), (255, 0, 0), 2)
    for line in vertical_lines:
        x1, y1, x2, y2 = map(int, line)
        cv2.line(debug_img, (x1, y1), (x2, y2), (0, 255, 0), 2)
    cv2.imwrite('detected_lines_debug.jpg', debug_img)

    # Find boxes using our intersection detection
    boxes = find_boxes_from_lines(horizontal_lines, vertical_lines, original.shape)

    # Process boxes to create results
    results = []
    for box in boxes:
        top, left, bottom, right = map(int, box)  # Ensure integer coordinates

        # Extract and process ROI
        roi = original[top:bottom, left:right]
        roi_gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        roi_gray = cv2.convertScaleAbs(roi_gray, alpha=1.5, beta=0)

        # OCR processing
        roi_pil = Image.fromarray(roi_gray)
        text = pytesseract.image_to_string(roi_pil, config=r'--oem 3 --psm 6').strip()

        results.append({
            'coordinates': (left, top, right - left, bottom - top),
            'text': text
        })

        # Draw box on original image
        cv2.rectangle(original, (left, top), (right, bottom), (0, 255, 0), 2)

    cv2.imwrite('annotated_form.jpg', original)
    return results


def ensure_form_boundaries(vertical_lines, horizontal_lines, image_shape):
    """
    Add form boundaries aligned with the extent of horizontal lines.

    We need to:
    1. Find the leftmost and rightmost x-coordinates where horizontal lines start/end
    2. Place our vertical boundaries at those positions
    3. Make these boundaries span the full height of the form
    """
    # Find the extent of horizontal lines
    min_x = float('inf')
    max_x = float('-inf')

    for line in horizontal_lines:
        x1, y1, x2, y2 = line
        min_x = min(min_x, x1, x2)
        max_x = max(max_x, x1, x2)

    # Check if we already have lines near these boundaries
    left_edge_exists = any(abs(line[0] - min_x) < 20 for line in vertical_lines)
    right_edge_exists = any(abs(line[0] - max_x) < 20 for line in vertical_lines)

    if not left_edge_exists:
        # Add left boundary at the start of horizontal lines
        vertical_lines.append([min_x, 0, min_x, image_shape[0]])

    if not right_edge_exists:
        # Add right boundary at the end of horizontal lines
        vertical_lines.append([max_x, 0, max_x, image_shape[0]])

    return vertical_lines

def do_lines_intersect(h_line, v_line):
    """
    Determine if a horizontal and vertical line segment actually intersect.
    
    Parameters:
        h_line: [x1, y1, x2, y2] of horizontal line segment
        v_line: [x1, y1, x2, y2] of vertical line segment
    
    Returns:
        bool: True if lines intersect, False otherwise
    """
    # Unpack coordinates
    h_x1, h_y1, h_x2, h_y2 = h_line
    v_x1, v_y1, v_x2, v_y2 = v_line

    # For true horizontal and vertical lines, intersection is simpler to check:
    # The vertical line's x must fall within the horizontal line's x range
    # AND the horizontal line's y must fall within the vertical line's y range
    x_overlaps = min(h_x1, h_x2) <= v_x1 <= max(h_x1, h_x2)
    y_overlaps = min(v_y1, v_y2) <= h_y1 <= max(v_y1, v_y2)

    return x_overlaps and y_overlaps


def find_boxes_from_lines(horizontal_lines, vertical_lines, original_shape):
    """
    Find boxes by examining how lines interact to form the form's structure.
    Each line can participate in forming multiple boxes - this is how actual forms work.

    Parameters:
        horizontal_lines: List of [x1, y1, x2, y2] horizontal line segments
        vertical_lines: List of [x1, y1, x2, y2] vertical line segments
        original_shape: Shape of the original image for size validation
    """
    boxes = []

    # Sort lines by their position to make processing logical
    # For horizontal lines, sort by y-coordinate (top to bottom)
    # For vertical lines, sort by x-coordinate (left to right)
    horizontal_lines.sort(key=lambda x: x[1])
    vertical_lines.sort(key=lambda x: x[0])

    print(f"\nProcessing lines to find boxes:")
    print(f"Found {len(horizontal_lines)} horizontal lines")
    print(f"Found {len(vertical_lines)} vertical lines")

    # For each pair of horizontal lines (potential top and bottom of boxes)
    for i in range(len(horizontal_lines) - 1):
        h1 = horizontal_lines[i]  # Upper horizontal line
        h2 = horizontal_lines[i + 1]  # Lower horizontal line
        h1_y = h1[1]  # y-coordinate of upper line
        h2_y = h2[1]  # y-coordinate of lower line

        # Find all vertical lines that intersect with both these horizontal lines
        valid_verticals = []
        for v_line in vertical_lines:
            v_x = v_line[0]  # x-coordinate of vertical line
            v_top = min(v_line[1], v_line[3])
            v_bottom = max(v_line[1], v_line[3])

            # Check if this vertical line spans between our horizontal lines
            if v_top <= h1_y <= v_bottom and v_top <= h2_y <= v_bottom:
                valid_verticals.append(v_line)

        # Now look at each pair of adjacent vertical lines that are valid
        if len(valid_verticals) >= 2:
            for j in range(len(valid_verticals) - 1):
                v1 = valid_verticals[j]  # Left vertical line
                v2 = valid_verticals[j + 1]  # Right vertical line

                # Create a box from these four lines
                left = min(v1[0], v1[2])
                right = max(v2[0], v2[2])
                top = h1_y
                bottom = h2_y

                width = right - left
                height = bottom - top

                # Filter boxes by size to avoid noise and invalid detections
                if (width > 30 and height > 20):
                    print(f"Found valid box:")
                    print(f"  Position: top={top}, left={left}, bottom={bottom}, right={right}")
                    print(f"  Size: {width}x{height} pixels")

                    boxes.append((int(top), int(left), int(bottom), int(right)))

    print(f"\nBox Detection Complete:")
    print(f"Total boxes found: {len(boxes)}")

    return boxes


def merge_nearby_lines(lines, threshold=20):
    """Merge lines that are close and parallel"""
    if not lines:
        return []

    merged = []
    used = set()

    for i, line1 in enumerate(lines):
        if i in used:
            continue

        current_group = [line1]
        used.add(i)

        for j, line2 in enumerate(lines):
            if j in used:
                continue

            # Check if lines are parallel and close
            if are_lines_similar(line1, line2, threshold):
                current_group.append(line2)
                used.add(j)

        # Merge lines in the group
        merged.append(merge_line_group(current_group))

    return merged


def are_lines_similar(line1, line2, threshold=20):
    """
    Determine if two lines are similar enough to be merged.

    Parameters:
        line1, line2: Lines in [x1, y1, x2, y2] format
        threshold: Maximum pixel distance to consider lines for merging

    Returns:
        bool: True if lines should be merged
    """
    # Extract coordinates
    x1, y1, x2, y2 = line1
    x3, y3, x4, y4 = line2

    # Calculate angles of both lines relative to horizontal
    angle1 = np.arctan2(y2 - y1, x2 - x1) * 180 / np.pi
    angle2 = np.arctan2(y4 - y3, x4 - x3) * 180 / np.pi

    # Lines must have similar angles (within 5 degrees) to be considered parallel
    angle_diff = abs(angle1 - angle2)
    if angle_diff > 5 and angle_diff < 355:  # Allow for angle wrapping around 360
        return False

    # For nearly horizontal lines, compare y-coordinates
    if abs(angle1) < 45 or abs(angle1) > 135:
        y_diff = min(abs(y1 - y3), abs(y1 - y4), abs(y2 - y3), abs(y2 - y4))
        return y_diff < threshold

    # For nearly vertical lines, compare x-coordinates
    else:
        x_diff = min(abs(x1 - x3), abs(x1 - x4), abs(x2 - x3), abs(x2 - x4))
        return x_diff < threshold


def merge_line_group(lines):
    """
    Merge a group of similar lines into a single line.

    Parameters:
        lines: List of lines in [x1, y1, x2, y2] format

    Returns:
        list: A single merged line [x1, y1, x2, y2]
    """
    if not lines:
        return None

    # For horizontal lines, we want the leftmost and rightmost x coordinates
    # For vertical lines, we want the highest and lowest y coordinates
    points = []
    for line in lines:
        points.append((line[0], line[1]))  # First endpoint
        points.append((line[2], line[3]))  # Second endpoint

    # Convert points to numpy array for easier manipulation
    points = np.array(points)

    # Calculate the average angle to determine if this is a horizontal or vertical group
    dx = points[:, 0].max() - points[:, 0].min()
    dy = points[:, 1].max() - points[:, 1].min()

    if abs(dx) > abs(dy):  # Horizontal line
        # Find leftmost and rightmost points
        left_point = points[points[:, 0].argmin()]
        right_point = points[points[:, 0].argmax()]
        # Use average y-coordinate for consistency
        avg_y = np.mean(points[:, 1])
        return [left_point[0], avg_y, right_point[0], avg_y]
    else:  # Vertical line
        # Find highest and lowest points
        top_point = points[points[:, 1].argmin()]
        bottom_point = points[points[:, 1].argmax()]
        # Use average x-coordinate for consistency
        avg_x = np.mean(points[:, 0])
        return [avg_x, top_point[1], avg_x, bottom_point[1]]


def process_pdf(pdf_path, output_dir):
    """Process entire PDF"""
    # Extract pages to TIFF
    tiff_files = extract_pdf_pages(pdf_path, os.path.join(output_dir, 'tiff_pages'))

    # Create directory for results
    results_dir = os.path.join(output_dir, 'results')
    os.makedirs(results_dir, exist_ok=True)

    # Process each page
    all_results = {}
    for tiff_file in tiff_files[:1]:

        original_bounds = get_image_bounds(tiff_file)
        # print("Original image bounds:")
        # print(f"Width x Height: {original_bounds['width']} x {original_bounds['height']}")
        # print(f"Top: {original_bounds['top']}")
        # print(f"Left: {original_bounds['left']}")
        # print(f"Bottom: {original_bounds['bottom']}")
        # print(f"Right: {original_bounds['right']}")

        page_num = os.path.basename(tiff_file).split('_')[1].split('.')[0]
        results = detect_lines_and_boxes(tiff_file)

        all_results[f'page_{page_num}'] = results

        # Rename output files to include page number
        if os.path.exists('annotated_form.jpg'):
            os.rename('annotated_form.jpg',
                      os.path.join(results_dir, f'annotated_page_{page_num}.jpg'))

    return all_results


def get_annotation_bounds(boxes):
    if not boxes:
        return None

    # Initialize with first box coordinates
    min_x = float('inf')
    min_y = float('inf')
    max_x = float('-inf')
    max_y = float('-inf')

    # Find min/max across all boxes
    for box in boxes:
        left, top, width, height = box
        right = left + width
        bottom = top + height

        min_x = min(min_x, left)
        min_y = min(min_y, top)
        max_x = max(max_x, right)
        max_y = max(max_y, bottom)

    return {
        'top': min_y,
        'left': min_x,
        'bottom': max_y,
        'right': max_x,
        'width': max_x - min_x,
        'height': max_y - min_y
    }

def get_image_bounds(image_path):
    img = cv2.imread(image_path)
    height, width = img.shape[:2]
    return {
        'top': 0,
        'left': 0,
        'bottom': height,
        'right': width,
        'width': width,
        'height': height
    }


def examine_image_properties(image_path):
    img = cv2.imread(image_path)
    print(f"Image shape: {img.shape}")
    print(f"Image dtype: {img.dtype}")

    # Try to access image metadata
    from PIL import Image
    with Image.open(image_path) as pil_img:
        print("DPI info:", pil_img.info.get('dpi'))
        print("All metadata:", pil_img.info)


# Also let's examine what happens after edge detection
def debug_edge_detection(image_path):
    img = cv2.imread(image_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    print(f"Original shape: {img.shape}")
    print(f"Edges shape: {edges.shape}")

    # Save edges for visual inspection
    cv2.imwrite('debug_edges.jpg', edges)


import csv
from datetime import datetime


def write_results_to_csv(all_results, output_dir):
    """
    Write detection results to a CSV file with a clear structure.

    Parameters:
        all_results: Dictionary with page numbers as keys and box results as values
        output_dir: Directory where the CSV file should be saved
    """
    # Create a timestamp for the filename to avoid overwriting previous results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = os.path.join(output_dir, f'form_analysis_{timestamp}.csv')

    # Open the file and create a CSV writer
    with open(csv_filename, 'w', newline='') as csvfile:
        # Define our column headers
        fieldnames = ['page_number', 'box_number', 'top', 'left', 'width', 'height', 'text_content']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header row
        writer.writeheader()

        # Process each page's results
        for page_key, boxes in all_results.items():
            # Extract page number from the page_key (format: "page_1")
            page_number = page_key.split('_')[1]

            # Write each box's information
            for box_number, box in enumerate(boxes, 1):
                # Get the box coordinates and text
                left, top, width, height = box['coordinates']
                text = box['text'].replace('\n', ' ').strip()  # Replace newlines with spaces

                # Write the row
                writer.writerow({
                    'page_number': page_number,
                    'box_number': box_number,
                    'top': top,
                    'left': left,
                    'width': width,
                    'height': height,
                    'text_content': text
                })

    print(f"Results written to: {csv_filename}")
    return csv_filename


# Usage
pdf_path = "/home/don/Documents/Temp/WW990/files_failing/010211547_202212_990PF_2023120422057614.pdf"
output_dir = '/home/don/Documents/Temp/WW990/processed_forms'
results_csv = os.path.join(output_dir, 'results.csv')
results = process_pdf(pdf_path, output_dir)
write_results_to_csv(results, output_dir)



# Print results
# for page, boxes in results.items():
#     print(f"\nResults for {page}:")
#     for box in boxes:
#         print(f"Box at {box['coordinates']}: {box['text']}")
