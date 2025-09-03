"""
ViewDirectivesRegistry - Default directive registration for Cara framework

This file provides default directive registration functionality.
"""


class ViewDirectivesRegistry:
    """Registry for default view directives."""

    @staticmethod
    def register_defaults(directives):
        """Register all default directives."""
        ViewDirectivesRegistry._register_control_structures(directives)
        ViewDirectivesRegistry._register_loops(directives)
        ViewDirectivesRegistry._register_template_inheritance(directives)
        ViewDirectivesRegistry._register_includes(directives)
        ViewDirectivesRegistry._register_comments(directives)
        ViewDirectivesRegistry._register_conditionals(directives)
        ViewDirectivesRegistry._register_forms(directives)

    @staticmethod
    def _register_control_structures(directives):
        """Register control structure directives."""

        def compile_if(expr):
            return f"if {expr}:"

        directives.register("if", compile_if)
        directives.register("elseif", lambda expr: f"elif {expr}:")
        directives.register("else", lambda expr: "else:")
        directives.register("endif", lambda expr: "pass")

    @staticmethod
    def _register_loops(directives):
        """Register loop directives."""

        def compile_foreach(expr):
            if " as " in expr:
                items, item = expr.split(" as ", 1)
                return f"""for {item.strip()}_temp in {items.strip()}:
    {item.strip()} = DataWrapper({item.strip()}_temp) if isinstance({item.strip()}_temp, dict) else {item.strip()}_temp"""
            return f"for item in {expr}:"

        directives.register("for", lambda expr: f"for {expr}:")
        directives.register("endfor", lambda expr: "pass")
        directives.register("foreach", compile_foreach)
        directives.register("endforeach", lambda expr: "pass")
        directives.register("while", lambda expr: f"while {expr}:")
        directives.register("endwhile", lambda expr: "pass")

    @staticmethod
    def _register_template_inheritance(directives):
        """Register template inheritance directives."""
        directives.register("extends", lambda expr: f"# extends {expr}")
        directives.register("section", lambda expr: f"# section {expr}")
        directives.register("endsection", lambda expr: "# endsection")
        directives.register("yield", lambda expr: f"# yield {expr}")
        directives.register("parent", lambda expr: "# parent")

    @staticmethod
    def _register_includes(directives):
        """Register include directives."""
        directives.register("include", lambda expr: f"# include {expr}")

    @staticmethod
    def _register_comments(directives):
        """Register comment directives."""
        directives.register("comment", lambda expr: "# comment start")
        directives.register("endcomment", lambda expr: "# comment end")

    @staticmethod
    def _register_conditionals(directives):
        """Register conditional directives."""

        def compile_isset(expr):
            return f"if '{expr}' in locals() and {expr} is not None:"

        def compile_empty(expr):
            return f"if not {expr}:"

        directives.register("isset", compile_isset)
        directives.register("endisset", lambda expr: "pass")
        directives.register("empty", compile_empty)
        directives.register("endempty", lambda expr: "pass")

    @staticmethod
    def _register_forms(directives):
        """Register form directives."""
        directives.register(
            "csrf",
            lambda expr: '__output__ += \'<input type="hidden" name="_token" value="csrf_token_here">\'',
        )

        def method_directive(expr):
            method_value = expr.strip("'\"")
            return f'__output__ += \'<input type="hidden" name="_method" value="{method_value}">\''

        directives.register("method", method_directive)
