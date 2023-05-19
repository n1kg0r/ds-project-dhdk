def clear_counter():
    with open('collection_counter.txt', 'w') as a:
        a.write('0')

    with open('manifest_counter.txt', 'w') as b:
        b.write('0')

    with open('canvas_counter.txt', 'w') as c:
        c.write('0')

clear_counter()