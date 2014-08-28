
class classproperty(property):
    """
    allow a class property to exist on the Orm

    NOTE -- this is read only, you can't write to the property

    example --

        class Foo(object):
            @classproperty
            def bar(cls):
                return 42

        Foo.bar # 42

    http://stackoverflow.com/questions/128573/using-property-on-classmethods
    http://stackoverflow.com/questions/5189699/how-can-i-make-a-class-property-in-python
    http://docs.python.org/2/reference/datamodel.html#object.__setattr__
    """
    def __get__(self, instance, cls):
        return self.fget(cls)

