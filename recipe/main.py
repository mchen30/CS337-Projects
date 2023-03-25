from data import *

recipy = None

def get_new_recipy():
    print('Please input a URL from AllRecipy.com: ')
    while recipy is None:
        url = input()
        try:
            recipy = get_recipe(url)
        except Exception as e:
            print('Invalid URL, please try again:')
    print('The extracted recipy is:')
    print(recipy)

def transform():
    print('Please specify a transformation, the options are:')
    print('\tVegetarian/Non-vegetarian')
    print('\tHealthy/Unhealthy')
    print('\tHalf/Double the serving size')
    print('\tMake Chinese/Mexican')
    print('\tLactose-free')
    while True:
        xform = input().lower()
        if xform == 'vegetarian':
            # change name
            recipy.to_vegetarian()
            break
        elif xform == 'non-vegetarian':
            recipy.to_non_vegetarian()
            break
        elif xform == 'healthy':
            recipy.to_healthy()
            break
        elif xform == 'unhealthy':
            recipy.to_unhealthy()
            break
        elif xform == 'half the serving size':
            recipy.half()
            break
        elif xform == 'double the serving size':
            recipy.double()
            break
        elif xform == 'make chinese':
            recipy.to_cuisine('chinese')
        elif xform == 'make mexican':
            recipy.to_cuisine('mexican')
        elif xform == 'lactose-free':
            recipy.lactose_free()
        else:
            print('Please choose from one of the transform options (case-insensitive):')
    print('The transformed recipy is:')
    print(recipy)


get_new_recipy()
transform()
while True:
    print('Would you like to start over or add another transformation to this recipy? (start over/continue)')
    choice = input().lower()
    if choice == 'start over':
        get_new_recipy()
    elif choice == 'continue':
        transform()
    else:
        print('The options are to start with a new recipy or to continue transforming the current one.\n'
              'Please type "start over" or "continue":')
