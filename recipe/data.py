import requests
import re
import unidecode
from bs4 import BeautifulSoup
from recipe import *


# request http file, parse title, ingredients, steps
def scrape(url):
    try:
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, 'html.parser')
        if soup.find('div', class_='loc article-post-header') is None:
            Exception('Invalid page')
    except Exception as e:
        Exception(e)
    return soup


def is_quantity(tag):
    return tag.has_attr('data-ingredient-quantity')


def is_unit(tag):
    return tag.has_attr('data-ingredient-unit')


def is_name(tag):
    return tag.has_attr('data-ingredient-name')


def get_recipe(url):
    soup = scrape(url)
    recipe_name = soup.find('h1', class_='comp type--lion article-heading mntl-text-block').contents[0].strip()
    ingredients = []
    ingredient_lst = soup.find_all('li', class_='mntl-structured-ingredients__list-item')
    for e in ingredient_lst:
        quantity = e.p.find(is_quantity).contents
        if len(quantity) > 0:
            quantity = quantity[0].strip()
        else:
            quantity = '0'
        unit = e.p.find(is_unit).contents
        if len(unit) > 0:
            unit = unit[0].strip()
        else:
            unit = ''
        name = unidecode.unidecode(e.p.find(is_name).contents[0].strip())
        ingredients.append(Ingredient(quantity, unit, name))
    steps = []
    steps_obj = soup.find('div', class_='comp recipe__steps mntl-block')
    step_lst = steps_obj.find_all('p', class_='comp mntl-sc-block mntl-sc-block-html')
    for e in step_lst:
        steps.append(unidecode.unidecode(e.contents[0].strip()))
    return Recipe(recipe_name, ingredients, steps)

