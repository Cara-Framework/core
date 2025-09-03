class Facade(type):
    def __getattr__(self, attribute, *args, **kwargs):
        from bootstrap import application

        # Handle IPython special methods to avoid unnecessary error logs
        if attribute.startswith("_ipython_") or attribute.startswith("_repr_"):
            raise AttributeError(
                f"'{self.__name__}' object has no attribute '{attribute}'"
            )

        # Handle other special methods that IPython might check
        if attribute in [
            "_ipython_canary_method_should_not_exist_",
            "_ipython_display_",
            "_repr_mimebundle_",
            "_repr_html_",
            "_repr_json_",
            "_repr_latex_",
            "_repr_javascript_",
            "_repr_png_",
            "_repr_jpeg_",
            "_repr_svg_",
        ]:
            raise AttributeError(
                f"'{self.__name__}' object has no attribute '{attribute}'"
            )

        try:
            return getattr(application.make(self.key), attribute)
        except Exception as e:
            self.get_logger().error(f"Facade {self.key} cannot be resolved: {e}")
            return None

    def __repr__(self):
        """Provide a clean representation for IPython."""
        return f"<Facade: {self.key}>"

    def __str__(self):
        """Provide a clean string representation."""
        return f"Facade({self.key})"

    def get_logger(self):
        from cara.logging import Logger

        return Logger(name=self.key)
