import re

proteins = {'beef': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'chicken': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'pork': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'sausage': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'bacon': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'salmon': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'trout': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'shrimp': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'egg': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            # vegetarian options
            'tofu': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Impossible Beef': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Impossible Chicken': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Impossible Sausage': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Impossible Meat': {'vegetarian': True, 'healthy': True, 'cuisine': None},}

vegetables = {'bok choy': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'tomato': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'celery': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'broccoli': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'lettuce': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'bell pepper': ,'carrot', 'corn', 'jalapeno pepper', 'cilantro', 'mushroom', 'onion'}

carbohydrates = {'white rice': {'vegetarian': True, 'healthy': False, 'cuisine': None},
                 'brown rice': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'flour': {'vegetarian': True, 'healthy': False, 'cuisine': None},
                 'whole-grain flour': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'noodles': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'corn tortillas'}

dairies = {'milk': {'vegetarian': False, 'healthy': True, 'cuisine': None},
           'Parmesan cheese': {'vegetarian': False, 'healthy': True, 'cuisine': None},
           'mozzarella cheese': {'vegetarian': False, 'healthy': True, 'cuisine': None},
           'provolone cheese': {'vegetarian': False, 'healthy': True, 'cuisine': None},
           'ricotta cheese':
           'cream cheese':
            'sharp Cheddar cheese'
            'cheddar cheese'
            'sour cream'
            'butter': }

condiments = {'oil': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'chicken broth': {'vegetarian': False, 'healthy': True, 'cuisine': None},
              'beef broth': {'vegetarian': False, 'healthy': False, 'cuisine': None},
              'garlic powder': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'black pepper': , 'oregano', 'spaghetti sauce', 'oyster sauce',
'soy sauce', 'salt', 'cumin', 'chicken bouillon', 'salsa'}

ingredient_info = {'protein': proteins,
                   'veggie': vegetables,
                   'carb': carbohydrates,
                   'dairy': dairies,
                   'condiments': condiments,}


class Recipe:
    def __init__(self, ingredients, steps):
        self.ingredients = ingredients
        self.steps = steps
        self.tools = list(set)

class Ingredient:
    def __init__(self, quantity, unit, name):
        # parse name, quantity, measurement
        # optional: other descriptors
        fracs = {'⅛': 0.125, '¼': 0.25, '⅜': 0.375 ,'½': 0.5, '⅝': 0.625, '¾': 0.75, '⅞': 0.875}
        # remove special chars
        match = re.search(r'^\d+', quantity)
        quant_digi = int(match.group())
        index_digi = match.span()[1]
        quant_frac = fracs[quantity[index_digi:]] if quantity[index_digi:] in fracs.keys() else 0
        self.quantity = quant_digi + quant_frac
        # sometimes no measurement is needed
        meas = ['cup', 'teaspoon', 'tablespoon', 'head', 'package', 'ounce', 'pound',
                'container', 'package', 'jar', ]
        desc = ['chopped', 'finely chopped', 'coarsely chopped']
        # extract quantity details from within parentheses
        self.measurement = unit
        self.alt_quant = int(re.search(r'^\d+', self.measurement).group())
        self.ingredient = name
        self.name =
        self.type =
        self.vegetarian = ingredient_info[self.name]['vegetarian']
        self.healthy = ingredient_info[self.name]['healthy']
        self.cuisine = ingredient_info[self.name]['cuisine']

    def __str__(self):
        # round to the nearest eighth
        fracs = {'⅛': 0.125, '¼': 0.25, '⅜': 0.375 ,'½': 0.5, '⅝': 0.625, '¾': 0.75, '⅞': 0.875}
        quant_frac = self.quantity % 1
        quant_digi = self.quantity - quant_frac
        quant_frac = quant_frac - quant_frac % 0.125
        quant_str = str(quant_digi)
        if quant_frac != 0:
            quant_str += fracs[quant_frac]
        return ' '.join([quant_str, self.measurement, self.name])

    def half(self):
        self.quantity /= 2

    def double(self):
        self.quantity *= 2

    def _fit_unit(self):
        # perform unit conversion if necessary
        pass

    def to_vegetarian(self):

    def to_non_vegetarian(self):

    def to_healthy(self):

    def to_unhealthy(self):

class Method:
    def __init__(self, text):
        # primary cooking methods, None if secondary cooking methods are used
        methods = ['sauté', 'bake', 'broil', 'boil', 'fry', 'poach', 'simmer']

        # chop[knife], grate, stir[saucepan,], shake, mince, crush, squeeze, heat(?), beat[bowl], scrape[bowl,]
        # mix, let stand; spoon, moisten, press, bring, overlap...

        tools = ['pan', 'grater', 'whisk', 'skillet', 'pot', 'oven', 'saucepan', 'broiler',
                 'bowl', 'microwave']
        self.default_tools = {'sauté': 'skillet', 'bake': 'oven', 'broil': 'broiler',
                         'boil': 'pot', 'fry': '', 'poach': 'pot', 'simmer': 'saucepan'}
        self.text = text
        self.name = None
        self.secondary = None
        for m in methods:
            if m in text: self.name = m
        self.tool = None
        if self.name is not None:
            self.tool = self.default_tools[self.name]
        for t in tools:
            if t in text: self.tool = t
        # [[['time', 'txt'], 'float(lower)', 'float(upper)', 'unit'],...]
        self.time = Method.find_time(text)

    def half(self):
        for t in self.time:
            if t[1] == t[2]:
                self.text.replace(' '.join(t[0]), ' '.join([str(t[1]/2), t[3]]))
            else:
                self.text.replace(' '.join(t[0]), ' '.join([str(t[1] / 2), 'to', str(t[2] / 2), t[3]]))

    def double(self):
        for t in self.time:
            if t[1] == t[2]:
                self.text.replace(' '.join(t[0]), ' '.join([str(t[1]*2), t[3]]))
            else:
                self.text.replace(' '.join(t[0]), ' '.join([str(t[1]* 2), 'to', str(t[2]*2), t[3]]))

    def replace_method(self, method, tool=None):
        re.sub(re.compile(f'{self.name}'), method, self.text)
        self.name = method
        if tool is None and method in self.default_tools.keys():
            re.sub(re.compile(f'{self.tool}'), self.default_tools[method], self.text)
            self.tool = self.default_tools[method]
        elif tool is not None:
            re.sub(re.compile(f'{self.tool}'), tool, self.text)
            self.tool = tool

    def __str__(self):
        return self.text

    @staticmethod
    def find_time(text):
        text = text.split(' ')
        time = []
        # find 'minute(s)', 'hour(s)'
        for i, w in enumerate(text):
            if 'minute' in w or 'hour' in w:
                limits = Method.extract_numerals(text[:i])
                if limits is not None:
                    limits.append(w)
                    limits[0].append(w)
                    time.append(limits)
        return time

    @staticmethod
    def extract_numerals(str_lst):
        upper, str_lst_remainder = Method.parse_number(str_lst)
        if upper is not None:
            if len(str_lst_remainder) > 1 and str_lst_remainder[-1] == 'to':
                str_lst_remainder = str_lst_remainder[:-1]
                lower, str_lst_remainder = Method.parse_number(str_lst_remainder)
            else:
                lower = None
            lower = upper if lower is None else lower
            return [str_lst[len(str_lst_remainder):], lower, upper]
        else:
            return None

    @staticmethod
    def parse_number(str_lst):
        if re.search(r'[^\d/]', str_lst[-1]) is None and len(re.findall(r'/', str_lst[-1])) <= 1:
            if len(str_lst) > 1 and str_lst[-1].find('/') != -1:
                elements = str_lst[-1].split('/')
                num = int(elements[0]) / int(elements[1]) + float(str_lst[-2])
                str_lst = str_lst[:-2]
            else:
                num = float(str_lst[-1])
                str_lst = str_lst[:-1]
        else:
            return None, str_lst
        return num, str_lst