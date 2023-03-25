import requests
import re
import unidecode
from bs4 import BeautifulSoup
from recipy import *


# request http file, parse title, ingredients, steps
def scrape(url):
    try:
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'html.parser')
        if soup.find('li', class_='next') is None:
            Exception('Empty page')
    except Exception as e:
        return e
    return soup


def is_quantity(tag):
    return tag.has_attr('data-ingredient-quantity')


def is_unit(tag):
    return tag.has_attr('data-ingredient-unit')


def is_name(tag):
    return tag.has_attr('data-ingredient-name')


def get_recipe(url):
    soup = scrape(url)
    name = soup.find('h1', class_='comp type--lion article-heading mntl-text-block')
    ingredients = []
    ingredient_lst = soup.find_all('li', class_='mntl-structured-ingredients__list-item')
    for e in ingredient_lst:
        quantity = e.p.find(is_quantity)
        unit = e.p.find(is_unit)
        name = e.p.find(is_name)
        ingredients.append(Ingredient(quantity.contents[0], unit.contents[0], unidecode.unidecode(name.contents[0])))
    steps = []
    step_lst = soup.find_all('p', class_='comp mntl-sc-block mntl-sc-block-html')
    for e in step_lst:
        steps.append(Method(unidecode.unidecode(e.contents[0].strip()), ingredients))
    return Recipe(name, ingredients, steps)

