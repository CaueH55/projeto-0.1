from myproject.greeter import greet


def test_greet():
    assert greet("Alice") == "Olá, Alice!"
