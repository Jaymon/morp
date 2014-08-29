import importlib

def get_class(full_python_class_path):
    """
    take something like some.full.module.Path and return the actual Path class object

    Note -- this will fail when the object isn't accessible from the module, that means
    you can't define your class object in a function and expect this function to work

    example -- THIS IS BAD --
        def foo():
            class FooCannotBeFound(object): pass
            # this will fail
            get_class("path.to.module.FooCannotBeFound")
    """
    module_name, class_name = full_python_class_path.rsplit('.', 1)
    m = importlib.import_module(module_name)
    return getattr(m, class_name)

