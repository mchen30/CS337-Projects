from data import *


def get_new_recipe():
    global recipe
    print('Please input a URL from AllRecipes.com: ')
    while recipe is None:
        url = input()
        try:
            recipe = get_recipe(url)
        except Exception as e:
            print(f'Invalid URL or empty webpage ({e}), please try again:')
    print('The extracted recipe is:\n')
    print(recipe)


def transform():
    global recipe
    assert isinstance(recipe, Recipe)
    print('Please specify a transformation, the options are:')
    print('\tTo vegetarian/non-vegetarian')
    print('\tTo healthy/unhealthy')
    print('\tHalf/double the serving size')
    print('\tTo Chinese/Mexican')
    print('\tTo lactose-free')
    while True:
        xform = input().lower()
        if xform == 'to vegetarian':
            # change name
            recipe.to_vegetarian()
            break
        elif xform == 'to non-vegetarian':
            recipe.to_non_vegetarian()
            break
        elif xform == 'to healthy':
            recipe.to_healthy()
            break
        elif xform == 'to unhealthy':
            recipe.to_unhealthy()
            break
        elif xform == 'half the serving size':
            recipe.half()
            break
        elif xform == 'double the serving size':
            recipe.double()
            break
        elif xform == 'to chinese':
            recipe.to_cuisine('chinese')
            break
        elif xform == 'to mexican':
            recipe.to_cuisine('mexican')
            break
        elif xform == 'to lactose-free':
            recipe.lactose_free()
            break
        else:
            print('Please choose from one of the transform options (case-insensitive):')
    print('The transformed recipe is:')
    print(recipe)


if __name__ == '__main__':
    recipe = None
    get_new_recipe()
    transform()
    while True:
        print('Would you like to start over or add another transformation to this recipe? (start over/continue)')
        choice = input().lower()
        if choice == 'start over':
            recipe = None
            get_new_recipe()
            transform()
        elif choice == 'continue':
            transform()
        else:
            print('Invalid option.')
