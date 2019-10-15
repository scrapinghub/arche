import re

from w3lib.html import remove_tags, remove_entities


UNITS_OF_MEASUREMENT = {'cap', 'cc', 'cnt', 'ds', 'fl-oz', 'g', 'lb', 'mg', 'ml',
                        'oz', 'pill', 'pk', 'tab', 'gm', 'l'}


def clean_output(input_text):
    if not isinstance(input_text, str):
        return input_text

    clean_text = remove_entities(remove_tags(input_text))
    clean_text = re.sub(r'\s+', ' ', clean_text)
    return clean_text.strip()


def only_alphanumeric(input_text):
    input_text = clean_output(input_text)
    input_text = re.sub(r'[^a-zA-Z0-9_\s]', '', input_text)
    input_text = re.sub(r'\s+', ' ', input_text)
    return input_text.strip()


def get_money(input_text):
    input_text = str(input_text).replace(',', '')
    input_text = re.sub(r'^\D+', '', input_text)
    match = re.match(r'(\d+(\.\d+)?)', input_text)
    if match:
        value = float(match.group(0).strip())
        return round(value, 2)


def boolean_output(value):
    return True if value else False


def long_to_short_unit_name(unit):
    long_to_short_unit_names = {
        'pound': 'lb',
        'lbs': 'lb',
        'ounce': 'oz',
        'ozs': 'oz',
        'ounces': 'oz',
        'fl oz': 'fl-oz',
        'fluid ounce': 'fl-oz',
        'gram': 'g',
        'capsules': 'cap',
        'capsule': 'cap',
        'tablet': 'tab',
        'tablets': 'tab',
        'pack': 'pk',
        'case': 'pk',
        'cans': 'pk',
        'ct': 'cnt',
        'count': 'cnt',
        'box': 'cnt',
        'month': 'cnt',
        'chew': 'cnt',
        'chewables': 'cnt',
        'liter': 'l',
        'litre': 'l',
        'chewable': 'cnt',
        'months': 'cnt',
        'grams': 'g',
        'can': 'pk',
    }
    unit = unit.lower().strip()
    return long_to_short_unit_names.get(unit, unit)


def standardize_uom(uom):
    uom = long_to_short_unit_name(uom)
    if uom in UNITS_OF_MEASUREMENT:
        return uom

    return None


def parse_quantity_uom(value):
    """Try to parse quantity and unit of measure from a string
    Curently looking for this units of measure: pill, tab, ml, mg, g, lb,
    ds, cnt, pk, oz, cc, fl-oz, cap, gm
    Please note that the preference is for package units.

    We follow a heuristic that the package quantity and uom will be the last match
    in a given string.
    So, we run a set of regexes (the package specific ones are the last ones) and
    stack all matches.
    At the end, we return the head/top of the stack.
    """
    value = value.lower()
    value = value.replace('fl oz', 'fl-oz')
    value = value.replace('fl. oz.', 'fl-oz')
    value = value.replace('sold per ', ' per ')
    value = value.replace(' per ', ' 1 ')
    value = value.replace(' / single', ' 1 cnt')

    regexes = [
        r'(^|[\s:\()])(?P<quantity>\d+\.?\d*)\s*(?P<unit>[a-zA-Z-]+)',
        r'(?P<unit>\w+) of (?P<quantity>\d+)',
        # e.g.: 24/5.8 oz. Cans
        r'(?P<quantity>\d+)/\d+\.?\d*\s*[a-zA-Z-]+\.*\s+(?P<unit>[a-zA-Z-]+)',
        r'(?P<quantity>\d+)/(?P<unit>\w+)',
        r'(?P<quantity>\d+) x 1',
        r'(?P<quantity>\d+)/\d+\s*\w+$',
        r'\((?P<quantity>\d+\.?\d*)\)\s+\d+\.?\d*\s*[a-zA-Z-.]+\s+(?P<unit>[a-zA-Z-]+)',
        r'(^|\s+)(?P<quantity>\.\d+)\s*(?P<unit>[a-zA-Z-]+)',
    ]

    pack_stack = []
    uom_stack = [(None, None)]
    for pattern in regexes:
        for match in re.finditer(pattern, value):
            uom = 'cnt' if len(match.groups()) == 1 else match.group('unit')
            uom = long_to_short_unit_name(uom)
            if uom in UNITS_OF_MEASUREMENT:
                stack = pack_stack if uom in {'pk', 'cnt', 'tab'} else uom_stack
                quantity = float(match.group('quantity'))
                quantity = f'{quantity:2.2f}'
                stack.append((quantity, uom))

    match_stack = uom_stack + pack_stack
    return match_stack.pop()


def parse_category_subcategory(breadcrumbs, skip_first=False):
    if not breadcrumbs or (skip_first and len(breadcrumbs) == 1):
        return (None, None)
    breadcrumbs = list(map(clean_output, breadcrumbs))

    if skip_first:
        breadcrumbs = breadcrumbs[1:]

    (category, *breadcrumbs) = breadcrumbs
    if category.lower() in {'pet', 'pets', 'pet supplies'}:
        (category, *breadcrumbs) = breadcrumbs

    subcategory = breadcrumbs[-1] if breadcrumbs else None

    return (category, subcategory)


def percentage_to_price(percentage, price):
    return round(percentage / 100 * price, 2)
