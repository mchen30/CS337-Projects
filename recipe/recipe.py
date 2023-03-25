import re
import random

proteins = {'beef': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'chicken': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'pork': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'sausage': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'bacon': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'steak': {'vegetarian': False, 'healthy': False, 'cuisine': None},
            'salmon': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'trout': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            'shrimp': {'vegetarian': False, 'healthy': True, 'cuisine': None},
            # vegetarian options
            'tofu': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Beyond Beef': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Beyond Chicken': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Beyond Sausage': {'vegetarian': True, 'healthy': True, 'cuisine': None},
            'Beyond Meat': {'vegetarian': True, 'healthy': True, 'cuisine': None},}

vegetables = {'bok choy': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'tomato': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'celery': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'broccoli': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'lettuce': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'bell pepper': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'carrot': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'jalapeno pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'poblano pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'serrano pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'arbol pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'habanero pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'guajillo pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'ancho pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'cascabel pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'pasilla pepper': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'cilantro': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'avocado': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'chayote': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'jicama': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'nopales': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'plantains': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'tomatillos': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'mushroom': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'onion': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'black olives': {'vegetarian': True, 'healthy': True, 'cuisine': None},}

carbohydrates = {'white rice': {'vegetarian': True, 'healthy': False, 'cuisine': None},
                 'brown rice': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'rice': {'vegetarian': True, 'healthy': False, 'cuisine': None},
                 'flour': {'vegetarian': True, 'healthy': False, 'cuisine': None},
                 'whole-grain flour': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'sticky rice flour': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
                 'noodles': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'whole-grain noodles': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'corn tortillas': {'vegetarian': True, 'healthy': True, 'cuisine': None},
                 'black beans': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'red beans': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'white beans': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'pinto beans': {'vegetarian': True, 'healthy': False, 'cuisine': 'mexican'},
                 'frijoles charros': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'refried beans': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'taco shells': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'corn kernels': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
                 'corn': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},}

dairies = {'milk': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'low-fat milk': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'soy milk': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'almond milk': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'ultra-filtered milk': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'yogurt': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'almond milk yogurt': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'low-fat yogurt': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'parmesan cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': False},
           'mozzarella cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'blue cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': False},
           'gruyere cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'fontina cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'feta cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'cottage cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'mascarpone cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'swiss cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'provolone cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'gouda cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': False},
           'ricotta cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'goat cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'cream cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'cheddar cheese': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'american cheese': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'cream': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'heavy cream': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'sour cream': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': True},
           'almond cream': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'coconut cream': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'butter': {'vegetarian': True, 'healthy': False, 'cuisine': None, 'lactose': True},
           'nut butter': {'vegetarian': True, 'healthy': True, 'cuisine': None, 'lactose': False},
           'queso fresco cheese': {'vegetarian': True, 'healthy': False, 'cuisine': 'mexican', 'lactose': True},
           'cotija cheese': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican', 'lactose': True},
           'crema cheese': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican', 'lactose': True},
           'panela cheese': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican', 'lactose': True},
           'queso de oaxaca cheese': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican', 'lactose': True},}

broths = {'chicken broth': {'vegetarian': False, 'healthy': True, 'cuisine': None},
          'beef broth': {'vegetarian': False, 'healthy': False, 'cuisine': None},
          'vegetable broth': {'vegetarian': True, 'healthy': True, 'cuisine': None},}

condiments = {'oil': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'egg': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'garlic powder': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'black peppercorn': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'black pepper': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'oregano': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'spaghetti sauce': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'oyster sauce': {'vegetarian': False, 'healthy': False, 'cuisine': None},
              'soy sauce': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'salt': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'sugar': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'cumin': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'chicken bouillon': {'vegetarian': False, 'healthy': False, 'cuisine': None},
              'vegetable bouillon': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'salsa verde': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'salsa roja': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'salsa brava': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'salsa tatemada': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'pesto': {'vegetarian': True, 'healthy': True, 'cuisine': 'mexican'},
              'tahini': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'mustard': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'ketchup': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'kimchi': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'sauerkraut': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'hummus': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'guacamole': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'apple cider': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'vinegar': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'honey': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'yeast': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'lemon': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'balsamic vinegar': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'vinaigrette': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'hot sauce': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'tamari': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'french dressing': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'ranch dressing': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'caesar dressing': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'thousand island dressing': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'mayonnaise': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'honey mustard dressing': {'vegetarian': True, 'healthy': True, 'cuisine': None},
              'barbecue sauce': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'syrup': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'margarine': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'teriyaki sauce': {'vegetarian': True, 'healthy': False, 'cuisine': 'japanese'},
              'sweetner': {'vegetarian': True, 'healthy': False, 'cuisine': None},
              'anise': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
              'shaoxing wine': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
              'rice vinegar': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
              'sesame oil': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
              'baijiu': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},
              'red bean paste': {'vegetarian': True, 'healthy': True, 'cuisine': 'chinese'},}

ingredient_info = {'protein': proteins,
                   'veggie': vegetables,
                   'carb': carbohydrates,
                   'dairy': dairies,
                   'broth': broths,
                   'condiment': condiments,}

ingredient_names = list(proteins.keys()) + list(vegetables.keys()) + list(carbohydrates.keys()) + list(dairies.keys()) \
                   + list(broths.keys()) + list(condiments.keys())


class Recipe:
    def __init__(self, name, ingredients, steps):
        self.name = name
        self.ingredients = ingredients
        self.steps = steps
        self.methods = []
        for s in steps:
            self.methods.append(self.extract_methods(s))

    def __str__(self):
        recipe = 'Recipe name:\n\t' + self.name + '\n'
        ingredients = '\nIngredients:\n\t' + '\n\t'.join([str(ing) for ing in self.ingredients]) + '\n'
        steps = '\nDirections:\n' + '\n'.join([f'Step {i}\n\t'
                                               + ''.join([str(self.methods[i][j]) for j in range(len(self.methods[i]))])
                                               for i in range(len(self.steps))])
        return recipe + ingredients + steps + '\n'

    def extract_methods(self, step):
        methods = []
        sub_steps = Recipe.partition(step)
        for s in sub_steps:
            methods.append(Method(s, self.ingredients))
        return methods

    @staticmethod
    def partition(step):
        matches = re.finditer(r'[\.;,]', step)
        start = 0
        sub_steps = []
        for m in matches:
            if start == len(step):
                break
            else:
                sub_steps.append(step[start:m.span()[1]])
                start = m.span()[1]
        return sub_steps

    def to_vegetarian(self):
        for i in self.ingredients:
            i.to_vegetarian()
        self.name = 'Vegetarian ' + self.name

    def to_non_vegetarian(self):
        for i in self.ingredients:
            i.to_non_vegetarian()
        self.name = 'Non-Vegetarian ' + self.name

    def to_healthy(self):
        for i in self.ingredients:
            i.to_healthy()
        self.name = 'Healthy ' + self.name

    def to_unhealthy(self):
        for i in self.ingredients:
            i.to_unhealthy()
        self.name = 'Unhealthy ' + self.name

    def half(self):
        for i in self.ingredients:
            i.half()
        for s in self.methods:
            for m in s:
                m.half()

    def double(self):
        for i in self.ingredients:
            i.double()
        for s in self.methods:
            for m in s:
                m.double()

    def to_cuisine(self, cuisine):
        if cuisine == 'chinese':
            carbs = False
            protein = False
            veggie = False
            soup = False
            ingredients = []
            steps = []
            if 'soup' in self.name: soup = True
            for ing in self.ingredients:
                if 'rice' in ing.name or 'bean' in ing.name or 'noodle' in ing.name: carbs = True
                elif ing.type == 'protein': protein = True
                elif ing.type == 'veggie': veggie = True
            if not soup and carbs and protein:
                # flour + protein -> pie
                self.name = 'Chinese ' + self.name + ' Pie'
                ingredients = [['2', '', 'large scallion, finely chopped'],
                               ['¼', 'teaspoon', 'salt'],
                               ['2', '', 'green onions'],
                               ['2', 'cups', 'all-purpose flour'],
                               ['½', 'cup', 'hot water'],
                               ['½', 'cup', 'room temperature water'],
                               ['½', '', 'white onion']]
                steps = ['Chop and mix the prepared food to make a protein-rich filling. Spread scallion white on top. Mix in chopped white onion.',
                         'Add salt in all purpose flour and then add hot water firstly. Gently stir the hot water in. And stir in cold water too. Roughly knead to form a dough. No need to be smooth. Cover and set aside for 30 minutes.',
                         'Re-knead the dough, it should be smooth very quickly. Cut into 2-ounce portions (10 pancakes from this batch) and then roll out to wrapper. Seal it completely. Avoid the edges meeting the filling, otherwise the sealing work is hard to complete.',
                         'Place the pancakes in a pan with oil, slightly press the center so the bottom can contact with the pan in better ways.',
                         'Heat until the side becomes crispy. Then turn over and fry the other side. Once the two side becomes well browned, slow down the fire and over with lid and let the pancake heat for another 3 to 5 minutes. This can further cook the inside part and make sure the inner part is well cooked too. Then remove the lid and heat both side for another ½ minute separately until the surface turn crispy again.',
                         'Transfer to oil paper to remove extra oil.']
            elif not soup and protein:
                # protein without flour
                self.name = 'Chinese Braised ("Red-Cooked") ' + self.name
                ingredients = [['3', 'cups', 'chicken broth'],
                               ['3', 'tablespoons', 'sugar'],
                               ['2', 'stars', 'anise'],
                               ['2', 'tablespoons', 'oil'],
                               ['5', 'tablespoons', 'soy sauce'],
                               ['1', 'tablespoon', 'Shaoxing wine']]

                steps = ['Add and heat oil in a pot. Transfer the cooked food to the pot. Add soy sauce, Shaoxing wine, 3 tablespoons of sugar and 2 stars of anise.',
                         'Add chicken broth or water, put the lid on, use maximum heat to bring the mixture to a boil; optionally, adjust the flavor of the mixture as desired. Do not add additional broth or water later on or it will impact the presentation of the food.',
                         'Use medium heat to braise the mixture for 2 hours or until the main ingredients are fully tender, and the flavors absorbed by the food.',
                         'Use maximum heat to bring the mixture to a boil. Cook for about 20 minutes or until the sauce has reduced significantly and thickened as desired. Use discretion to decide when to turn off the heat and how much sauce to retain. Transfer the food and sauce to a dish.']
            elif not soup and veggie:
                # Blanched vegetables
                self.name = 'Chinese Blanched ' + self.name
                # vegetables -> broil + soy sauce, ginger
                ingredients = [['2', 'tablespoons', 'light soy sauce'],
                               ['1', 'teaspoon', 'grated fresh ginger'],
                               ['1', '', 'large scallion, finely chopped'],
                               ['2', 'tablespoons', 'oil'],
                               ['1', 'tablespoon', 'Shaoxing wine'],
                               ['1', 'tablespoon', 'baijiu'],]
                steps = ['Add 1 tablespoon of baijiu, 1 tablespoon of Shaoxing wine, 1 teaspoon of grated fresh ginger, and a reasonable amount of water to a wok, use high heat to bring to a boil.',
                         'Add 2 tablespoons of cooking oil to the boiling mix. Add the vegetables and other main ingredients to the boil for about 3 minutes in maximum heat until it is cooked.',
                         'Transfer the food to a plate, serve with 2 tablespoons of light soy sauce.']
            elif not soup and not veggie:
                # no protein/vegetables -> carbs: sticky rice + dairy: red bean paste
                self.name = 'Chinese ' + self.name + ' Sticky Rice Cake w/ Red Bean Paste'
                for ing in self.ingredients:
                    if ing.type == 'carb':
                        ing.change_ingredient('sticky rice flour')
                    elif ing.type == 'dairy':
                        ing.change_ingredient('sweetened red bean paste')
            else:
                # soup -> tofu + hot & sour
                self.name = 'Hot and Sour ' + self.name
                ingredients = [['2', 'tablespoons', 'soy sauce'],
                               ['¼', 'cup', 'seasoned rice vinegar'],
                               ['1', 'teaspoon', 'ground white pepper'],
                               ['¼', 'teaspoon', 'sesame oil'],
                               ['¼', 'cup', 'grated carrot'],
                               ['½', 'cup', 'bamboo shoots'],
                               ['1', 'cup', 'cubed tofu'],
                               ['2', 'cups', 'chicken broth'],
                               ['2', 'tablespoons', 'thinly sliced red bell pepper']]
                steps = ['Whisk soy sauce, vinegar, ground white pepper, and sesame oil together in a small bowl for hot and sour mixture; set aside until needed.',
                         'Stir in carrot, red pepper, bamboo shoots, tofu, and the hot and sour mixture to the prepared soup. Add additional chicken broth to taste. Let simmer for 5 minutes. Transfer the soup to a bowl.']
            for ing in ingredients:
                self.ingredients.append(Ingredient(*ing))
            self.steps.extend(steps)
            for s in steps:
                self.methods.append(self.extract_methods(s))
        elif cuisine == 'mexican':
            # replace vegetables/peppers with ones found in traditional mexican cuisine
            # replace noodles and flour with corn
            # add two more types of beans
            # add one type of salsa
            peppers = []
            mex_veg = []
            for k in vegetables.keys():
                if 'pepper' in k:
                    peppers.append(k)
                elif vegetables[k]['cuisine'] == 'mexican':
                    mex_veg.append(k)
            for ing in self.ingredients:
                if ing.type == 'veggie':
                    if 'pepper' in ing.name:
                        ing.change_ingredient(peppers[random.randint(0, len(peppers) - 1)])
                    else:
                        ing.change_ingredient(mex_veg[random.randint(0, len(mex_veg) - 1)])
                elif 'flour' in ing.name or 'noodles' in ing.name:
                    ing.change_ingredient('corn kernels')
            tacos = False
            ingredients = []
            if random.randint(0, 1) == 0:
                tacos = True
            beans = []
            salsas = []
            for k in carbohydrates.keys():
                if 'beans' in k:
                    beans.append(k)
            for i in random.sample(range(len(beans)), 2):
                ingredients.append(['1', '(10 ounce) can', beans[i]])
            for k in condiments.keys():
                if 'salsa' in k:
                    salsas.append(k)
            ingredients.append(['½', 'cup', salsas[random.randint(0, len(salsas) - 1)]])
            steps = ['Bake the beans separately in a large bowl and mix with the main ingredients.']
            if tacos:
                self.name = 'Mexican ' + self.name + ' Tacos'
                ingredients.append(['10', '', 'taco shells, warmed'])
                steps.append('Serve the proteins and salsa with warmed taco shells.')
            else:
                self.name = 'Mexican ' + self.name + ' Tortillas'
                ingredients.append(['10', '', 'corn tortillas, warmed'])
                steps.append('Serve the proteins and salsa with warmed corn tortillas.')
            for ing in ingredients:
                self.ingredients.append(Ingredient(*ing))
            self.steps.extend(steps)
            for s in steps:
                self.methods.append(self.extract_methods(s))
        else:
            Exception('Cuisine not implemented')

    def lactose_free(self):
        lf_milk = []
        lf_yogurt = []
        lf_cheese = []
        lf_cream = []
        lf_butter = []
        for k in dairies.keys():
            if 'milk' in k and not dairies[k]['lactose']:
                lf_milk.append(k)
            elif 'yogurt' in k and not dairies[k]['lactose']:
                lf_yogurt.append(k)
            elif 'cheese' in k and not dairies[k]['lactose']:
                lf_cheese.append(k)
            elif 'cream' in k and not dairies[k]['lactose']:
                lf_cream.append(k)
            elif 'butter' in k and not dairies[k]['lactose']:
                lf_butter.append(k)
        for ing in self.ingredients:
            if ing.type == 'dairy' and dairies[ing.name]['lactose']:
                if 'milk' in ing.name:
                    ing.change_ingredient(lf_milk[random.randint(0, len(lf_milk) - 1)])
                elif 'yogurt' in ing.name:
                    ing.change_ingredient(lf_yogurt[random.randint(0, len(lf_yogurt) - 1)])
                elif 'cheese' in ing.name:
                    ing.change_ingredient(lf_cheese[random.randint(0, len(lf_cheese) - 1)])
                elif 'cream' in ing.name:
                    ing.change_ingredient(lf_cream[random.randint(0, len(lf_cream) - 1)])
                elif 'butter' in ing.name:
                    ing.change_ingredient(lf_butter[random.randint(0, len(lf_butter) - 1)])


class Ingredient:
    def __init__(self, quantity, unit, name):
        # parse name, quantity, measurement
        # REQUIRE: params are stripped strs
        # optional: other descriptors
        fracs = {'⅛': 0.125, '¼': 0.25, '⅜': 0.375 ,'½': 0.5, '⅝': 0.625, '¾': 0.75, '⅞': 0.875}
        # remove special chars
        match = re.search(r'^\d+', quantity)
        if match is not None:
            quant_digi = int(match.group())
            index_digi = match.span()[1]
        else:
            quant_digi = 0
            index_digi = 0
        quant_frac = fracs[quantity[index_digi:]] if quantity[index_digi:] in fracs.keys() else 0
        # self.text= ' '.join([quantity, unit, name])
        self.quantity = quant_digi + quant_frac
        self.quant_str = ' '.join([quantity, unit]) if self.quantity > 0 else ''
        # sometimes no measurement is needed
        meas = ['cup', 'teaspoon', 'tablespoon', 'head', 'package', 'ounce', 'pound',
                'container', 'package', 'jar', ]
        desc = ['chopped', 'finely chopped', 'coarsely chopped']
        # extract quantity details from within parentheses
        self.measurement = unit
        alt_quant_match = re.search(r'^\d+', self.measurement)
        self.alt_quant = int(alt_quant_match.group()) if alt_quant_match is not None else None
        self.ingredient = name
        # short name
        self.name = name
        self.default_name = True
        length = 0
        for n in ingredient_names:
            if n.lower() in self.ingredient.lower():
                n_len = len(n.split(' '))
                if n_len > length:
                    self.name = n
                    self.default_name = False
                    length = n_len
        self.type = self.vegetarian = self.healthy = self.cuisine = None
        self.get_info()
        self.methods = []

    def get_info(self):
        if not self.default_name:
            if self.name in proteins.keys(): self.type = 'protein'
            elif self.name in vegetables.keys(): self.type = 'veggie'
            elif self.name in carbohydrates.keys(): self.type = 'carb'
            elif self.name in dairies.keys(): self.type = 'dairy'
            elif self.name in broths.keys(): self.type = 'broth'
            elif self.name in condiments.keys(): self.type = 'condiment'
            self.vegetarian = ingredient_info[self.type][self.name]['vegetarian']
            self.healthy = ingredient_info[self.type][self.name]['healthy']
            self.cuisine = ingredient_info[self.type][self.name]['cuisine']

    def add_method(self, method):
        self.methods.append(method)

    def change_ingredient(self, new):
        self.default_name = True
        new_name = new
        length = 0
        for n in ingredient_names:
            if n.lower() in new.lower():
                n_len = len(n.split(' '))
                if n_len > length:
                    new_name = n
                    self.default_name = False
                    length = n_len
        for m in self.methods:
            m.update_text(self.name, new_name)
        self.ingredient = self.ingredient.replace(self.name, new)
        self.name = new_name
        self.get_info()

    def __str__(self):
        # round to the nearest eighth
        quant_str = self.quantity_str()
        return ' '.join([quant_str, self.measurement, self.ingredient])

    def to_vegetarian(self):
        if self.vegetarian:
            return
        elif self.type == 'protein':
            veg_options = [k for k in proteins.keys() if proteins[k]['vegetarian']]
            for o in veg_options:
                if self.name in o.lower():
                    self.change_ingredient(o)
                    return
            # replace other red meats with Impossible Meat
            if self.name in ['bacon', 'steak',]:
                self.change_ingredient('Beyond Meat')
            else:
                self.change_ingredient('tofu')
        elif self.type == 'broth':
            self.change_ingredient('vegetable broth')
        elif self.type == 'condiment':
            if self.name == 'chicken bouillon':
                self.change_ingredient('vegetable bouillon')
            elif self.name == 'oyster sauce':
                self.change_ingredient('teriyaki sauce')
            else:
                self.change_ingredient('spaghetti sauce')

    def to_non_vegetarian(self):
        if not self.vegetarian:
            return True
        elif self.type == 'protein':
            nonveg_options = [k for k in proteins.keys() if not proteins[k]['vegetarian']]
            for o in nonveg_options:
                if o in self.name:
                    self.change_ingredient(o)
                    return True
            self.change_ingredient(nonveg_options[random.randint(0, len(nonveg_options)-1)])
            return True
        elif self.type == 'veggie':
            return False
        elif self.type == 'carb':
            return False
        elif self.type == 'dairy':
            return False
        elif self.type == 'broth':
            nonveg_options = [k for k in broths.keys() if not broths[k]['vegetarian']]
            self.change_ingredient(nonveg_options[random.randint(0, len(nonveg_options)-1)])
            return True
        elif self.type == 'condiment':
            nonveg_options = [k for k in condiments.keys() if not condiments[k]['vegetarian']]
            self.change_ingredient(nonveg_options[random.randint(0, len(nonveg_options)-1)])
            return True

    def to_healthy(self):
        if self.healthy:
            return True
        elif self.type == 'protein':
            return self.to_vegetarian()
        elif self.type == 'carb':
            healthy_options = [k for k in carbohydrates.keys() if carbohydrates[k]['healthy']]
            for o in healthy_options:
                if self.name in o:
                    self.change_ingredient(o)
                    return True
            if self.name == 'white rice':
                self.change_ingredient('brown rice')
                return True
        elif self.type == 'dairy':
            healthy_options = [k for k in dairies.keys() if dairies[k]['healthy']]
            for o in healthy_options:
                if self.name in o:
                    self.change_ingredient(o)
                    return True
            self.change_ingredient(healthy_options[random.randint(0, len(healthy_options)-1)])
            return True
        elif self.type == 'broth':
            healthy_options = [k for k in broths.keys() if broths[k]['healthy']]
            self.change_ingredient(healthy_options[random.randint(0, len(healthy_options)-1)])
            return True
        elif self.type == 'condiment':
            if self.name == 'salt' or self.name == 'oil' or self.name == 'sugar' or self.name == 'sweetner':
                self.half()
                return True
            else:
                healthy_options = [k for k in condiments.keys() if condiments[k]['healthy']]
                self.change_ingredient(healthy_options[random.randint(0, len(healthy_options) - 1)])
                return True

    def to_unhealthy(self):
        if not self.healthy:
            return True
        elif self.type == 'protein':
            if self.name == 'egg':
                return False
            else:
                unhealthy_options = [k for k in proteins.keys() if not proteins[k]['healthy']]
                self.change_ingredient(unhealthy_options[random.randint(0, len(unhealthy_options)-1)])
                return True
        elif self.type == 'carb':
            unhealthy_options = [k for k in carbohydrates.keys() if not carbohydrates[k]['healthy']]
            for o in unhealthy_options:
                if o in self.name:
                    self.change_ingredient(o)
                    return True
            self.change_ingredient(unhealthy_options[random.randint(0, len(unhealthy_options)-1)])
            return True
        elif self.type == 'dairy':
            unhealthy_options = [k for k in dairies.keys() if not dairies[k]['healthy']]
            for o in unhealthy_options:
                if o in self.name:
                    self.change_ingredient(o)
                    return True
            self.change_ingredient(unhealthy_options[random.randint(0, len(unhealthy_options)-1)])
            return True
        elif self.type == 'broth':
            unhealthy_options = [k for k in broths.keys() if not broths[k]['healthy']]
            self.change_ingredient(unhealthy_options[random.randint(0, len(unhealthy_options)-1)])
            return True
        elif self.type == 'condiment':
            if self.name == 'salt' or self.name == 'oil' or self.name == 'sugar' or self.name == 'sweetner':
                self.double()
                return True
            else:
                unhealthy_options = [k for k in condiments.keys() if not condiments[k]['healthy']]
                self.change_ingredient(unhealthy_options[random.randint(0, len(unhealthy_options) - 1)])
                return True

    def quantity_str(self):
        # round to the nearest eighth
        fracs = {0.125: '⅛', 0.25: '¼', 0.375: '⅜', 0.5: '½', 0.625: '⅝', 0.75: '¾', 0.875: '⅞'}
        quant_frac = self.quantity % 1
        quant_digi = int(self.quantity - quant_frac)
        quant_frac = quant_frac - quant_frac % 0.125
        quant_str = str(quant_digi) if quant_digi != 0 else ''
        if quant_frac != 0:
            quant_str += fracs[quant_frac]
        return quant_str

    def half(self):
        self.quantity /= 2
        new_str = self.quantity_str()
        for m in self.methods:
            m.update_text(self.quant_str, new_str)
        self.quant_str = new_str

    def double(self):
        self.quantity *= 2
        new_str = self.quantity_str()
        for m in self.methods:
            m.update_text(self.quant_str, new_str)
        self.quant_str = new_str

    def _fit_unit(self):
        # perform unit conversion if necessary
        return


class Method:
    def __init__(self, text, ingredients):
        # primary cooking methods, None if secondary cooking methods are used
        methods = ['sauté', 'bake', 'broil', 'boil', 'fry', 'poach', 'simmer', 'blanch']

        # chop[knife], grate, stir[saucepan,], shake, mince, crush, squeeze, heat(?), beat[bowl], scrape[bowl,]
        # mix, let stand; spoon, moisten, press, bring, overlap...

        tools = ['pan', 'grater', 'whisk', 'skillet', 'pot', 'oven', 'saucepan', 'broiler',
                 'bowl', 'microwave', 'wok']
        self.default_tools = {'sauté': 'skillet', 'bake': 'oven', 'broil': 'broiler',
                              'boil': 'pot', 'fry': 'pan', 'poach': 'pot', 'simmer': 'saucepan',
                              'blanch': 'pot', 'braise': 'wok'}
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
        self.ingredients = []
        for ing in ingredients:
            if ing.name in text:
                self.ingredients.append(ing)
                ing.add_method(self)
        # [[['time', 'txt'], 'float(lower)', 'float(upper)', 'unit'],...]
        self.time = Method.find_time(text)

    def __str__(self):
        return self.text

    def update_method(self, method, tool):
        self.update_text(self.name, method)
        self.update_text(self.tool, tool)
        self.name = method
        self.tool = tool

    def update_text(self, orig, new):
        # find and replace occurrences of ingredients and method/tool in text
        # run during every method/tool update, and ingredient update
        if len(orig) > 0 and len(new) > 0:
            if not orig[0].isupper():
                self.text = re.sub(orig, new, self.text)
                self.text = re.sub(orig[0].upper()+orig[1:], new[0].upper()+new[1:], self.text)
            else:
                self.text = re.sub(orig, new, self.text)
                self.text = re.sub(orig[0].lower()+orig[1:], new, self.text)

    def half(self):
        for t in self.time:
            if t[1] == t[2]:
                repl = ' '.join([str(t[1]/2), t[3]])
                self.text = self.text.replace(' '.join(t[0]), repl)
            else:
                repl = ' '.join([str(t[1]/2), 'to', str(t[2]/2), t[3]])
                self.text = self.text.replace(' '.join(t[0]), repl)
            t[1] /= 2
            t[2] /= 2
            t[0] = repl.split(' ')

    def double(self):
        for t in self.time:
            if t[1] == t[2]:
                repl = ' '.join([str(t[1]*2), t[3]])
                self.text = self.text.replace(' '.join(t[0]), repl)
            else:
                repl = ' '.join([str(t[1]*2), 'to', str(t[2]*2), t[3]])
                self.text = self.text.replace(' '.join(t[0]), repl)
            t[1] *= 2
            t[2] *= 2
            t[0] = repl.split(' ')

    def replace_method(self, method, tool=None):
        self.text = re.sub(re.compile(f'{self.name}'), method, self.text)
        self.name = method
        if tool is None and method in self.default_tools.keys():
            self.text = re.sub(re.compile(f'{self.tool}'), self.default_tools[method], self.text)
            self.tool = self.default_tools[method]
        elif tool is not None:
            self.text = re.sub(re.compile(f'{self.tool}'), tool, self.text)
            self.tool = tool

    @staticmethod
    def find_time(text):
        text = text.split(' ')
        time = []
        # find 'minute(s)', 'hour(s)'
        for i, w in enumerate(text):
            if 'minute' in w or 'hour' in w:
                if i > 0 and text[i - 1] == 'more':
                    limits = Method.extract_numerals(text[:i - 1])
                    if limits is not None:
                        limits.append(w)
                        limits[0].extend(['more', w])
                        time.append(limits)
                else:
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
            if len(str_lst_remainder) > 1 and (str_lst_remainder[-1] == 'to' or str_lst_remainder[-1] == 'or'):
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
