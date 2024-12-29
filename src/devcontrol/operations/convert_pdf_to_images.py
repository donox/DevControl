from pdf2image import convert_from_path


def convert_pdf_to_images(input_path):
    """Convert a PDF file into a list of images."""
    try:
        images = convert_from_path(input_path, dpi=300)
        # Optionally save images or return directly
        result =  [image for image in images]
        return result
    except Exception as e:
        raise ValueError(f"Error converting PDF to images: {e}")
