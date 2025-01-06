import os
import shutil
import lxml.etree as ET
import csv
from datetime import datetime

def filter_driver(directory_generator, **kwargs):
    try:
        for input_directory in directory_generator:
            result = process_directory(input_directory, **kwargs)
            yield result
    except StopIteration:
        return None
    except Exception as e:
        log_error(f"Critical error during filtering: {e}")
        raise

def process_directory(input_directory, **kwargs):
    try:
        raw_directory = os.path.join(input_directory, 'raw')
        processed_directory = os.path.join(input_directory, 'processed')
        result_directory = os.path.join(input_directory, 'result')

        # Ensure subdirectories exist
        os.makedirs(processed_directory, exist_ok=True)
        os.makedirs(result_directory, exist_ok=True)

        for filename in os.listdir(raw_directory):
            raw_filepath = os.path.join(raw_directory, filename)

            if os.path.isfile(raw_filepath):
                filter_file(raw_filepath, input_directory, **kwargs)

        return True

    except Exception as e:
        log_error(f"Error processing directory '{input_directory}': {e}")
        raise

def filter_file(filepath, input_directory, **kwargs):
    try:
        xpaths_file = kwargs.get('xpaths')
        if not xpaths_file:
            raise ValueError("Missing 'xpaths' in kwargs.")

        # Load XPath expressions from the CSV file
        xpaths = []
        with open(xpaths_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 3:
                    xpath = row[0].strip()
                    filter_expression = row[1].strip() if len(row) > 1 else None
                    true_action = row[2].strip()
                    false_action = row[3].strip() if len(row) > 3 else None
                    xpaths.append((xpath, filter_expression, true_action, false_action))

        # Parse the input file
        tree = ET.parse(filepath)
        root = tree.getroot()

        # Extract namespaces from the XML root
        namespaces = root.nsmap

        # Prepare XML result document
        result_root = ET.Element('Results')

        for xpath, filter_expression, true_action, false_action in xpaths:
            # Rewrite XPath with namespace prefix if needed
            rewritten_xpath = rewrite_xpath_with_namespace(xpath, namespaces)

            matches = root.xpath(rewritten_xpath, namespaces=namespaces)

            for match in matches:
                filter_result = True
                if filter_expression:
                    try:
                        filter_type, expression = filter_expression.split(':', 1)
                        x = match.text.strip() if isinstance(match, ET._Element) and match.text else ""

                        if filter_type == 'str':
                            filter_result = eval(expression)
                        elif filter_type == 'num':
                            try:
                                x = float(x)
                                filter_result = eval(expression)
                            except ValueError:
                                log_error(f"Value '{x}' is not numeric for filter '{filter_expression}'")
                                filter_result = False
                        else:
                            log_error(f"Unknown filter type '{filter_type}' in filter '{filter_expression}'")
                            filter_result = False

                    except Exception as e:
                        log_error(f"Error evaluating filter for '{xpath}': {e}")
                        raise

                action = true_action if filter_result else false_action

                if action == 'IGNORE':
                    os.remove(filepath)
                    # log_error(f"File '{filepath}' ignored based on filter for '{xpath}'.")
                    return
                elif action == 'RECORD':
                    cleaned_match = remove_namespace(match)
                    copied_element = ET.Element(cleaned_match.tag, attrib=cleaned_match.attrib)
                    copied_element.text = cleaned_match.text
                    copied_element.tail = cleaned_match.tail
                    for child in cleaned_match:
                        copied_element.append(child)
                    result_root.append(copied_element)

        # Save results to XML file
        result_directory = os.path.join(input_directory, 'result')
        os.makedirs(result_directory, exist_ok=True)

        result_filename = os.path.basename(filepath).rsplit('.', 1)[0] + '.xml'
        result_filepath = os.path.join(result_directory, result_filename)

        with open(result_filepath, 'wb') as f:
            f.write(ET.tostring(result_root, pretty_print=True, encoding='utf-8', xml_declaration=True))
        # print(f"Result found: {result_filepath}")

        # Move the processed file to the 'processed' directory
        processed_directory = os.path.join(input_directory, 'processed')
        os.makedirs(processed_directory, exist_ok=True)
        shutil.move(filepath, os.path.join(processed_directory, os.path.basename(filepath)))

    except Exception as e:
        log_error(f"Error processing file '{filepath}': {e}")
        raise

def remove_namespace(element):
    """
    Removes namespace information from an element and its children.
    Returns a copy of the element without namespaces.
    """
    for elem in element.iter():
        if elem.tag.startswith("{"):
            elem.tag = elem.tag.split('}', 1)[1]  # Remove namespace
        attributes = {key.split('}', 1)[-1]: value for key, value in elem.attrib.items()}  # Remove namespace from attributes
        elem.attrib.clear()
        elem.attrib.update(attributes)  # Restore attributes without namespaces
    return element

def rewrite_xpath_with_namespace(xpath, namespaces):
    """
    Rewrite an XPath expression to include a namespace prefix for all elements.
    Args:
        xpath (str): The original XPath expression.
        namespaces (dict): The namespace mapping from the XML root.
    Returns:
        str: The rewritten XPath expression with namespace prefixes.
    """
    if None in namespaces:
        namespaces['ns'] = namespaces.pop(None)  # Map default namespace to 'ns'

    parts = xpath.split('/')
    rewritten = []
    for part in parts:
        if part and not part.startswith('@'):  # Skip attributes and empty parts
            rewritten.append(f"ns:{part}")
        else:
            rewritten.append(part)  # Keep attributes and slashes as-is
    return '/'.join(rewritten)


def log_error(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] ERROR: {message}")
