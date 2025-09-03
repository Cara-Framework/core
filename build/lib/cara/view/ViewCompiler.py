"""
View Compiler - Template compiler for Cara view engine

This file provides template compilation functionality.
"""

import re
from typing import Dict, List


class ViewCompiler:
    """Template compiler for converting templates to Python code."""

    def __init__(self):
        """Initialize view compiler."""
        self.indent_level = 0
        self.sections = {}
        self.extends_template = None

    def compile(self, template: str, directives) -> str:
        """Compile template to Python code."""
        self.reset()

        # Remove comments first
        template = self.remove_comments(template)

        lines = template.split("\n")
        compiled_lines = ["__output__ = ''"]

        for i, line in enumerate(lines):
            line_compiled = self.compile_line(line, directives)
            compiled_lines.extend(line_compiled)

            # Check if we need a pass statement after control structures
            if self.is_directive_line(line) and self.indent_level > 0:
                # Look ahead to see if next line has content
                next_line_has_content = False
                for j in range(i + 1, len(lines)):
                    next_line = lines[j].strip()
                    if next_line and not next_line.startswith("@end"):
                        next_line_has_content = True
                        break
                    elif next_line.startswith("@end"):
                        break

                # If no content follows, add pass
                if not next_line_has_content:
                    compiled_lines.append("    " * self.indent_level + "pass")

        return "\n".join(compiled_lines)

    def remove_comments(self, template: str) -> str:
        """Remove @comment...@endcomment blocks."""
        # Remove comment blocks
        pattern = r"@comment.*?@endcomment"
        return re.sub(pattern, "", template, flags=re.DOTALL)

    def compile_line(self, line: str, directives) -> List[str]:
        """Compile a single line of template."""
        compiled_lines = []

        # Handle different types of content
        if self.is_directive_line(line):
            compiled_lines.extend(self.compile_directive_line(line, directives))
        elif self.has_raw_echo_statements(line) and self.has_echo_statements(line):
            # Handle mixed echo statements
            compiled_lines.extend(self.compile_mixed_echo_line(line))
        elif self.has_raw_echo_statements(line):
            compiled_lines.extend(self.compile_raw_echo_line(line))
        elif self.has_echo_statements(line):
            compiled_lines.extend(self.compile_echo_line(line))
        else:
            # Regular HTML content
            stripped_line = line.strip()
            if stripped_line:
                escaped_line = stripped_line.replace('"', '\\"').replace("'", "\\'")
                compiled_lines.append(
                    "    " * self.indent_level + f"__output__ += '{escaped_line}\\n'"
                )
            # Don't add anything for empty lines - let the main compile method handle pass statements

        return compiled_lines

    def is_directive_line(self, line: str) -> bool:
        """Check if line contains a directive."""
        # More sophisticated directive detection that ignores:
        # - CSS rules like @keyframes, @media, @import
        # - Email addresses like user@example.com
        # - URLs with @ symbols

        # Skip CSS rules
        css_rules = [
            "keyframes",
            "media",
            "import",
            "charset",
            "font-face",
            "supports",
            "page",
            "namespace",
        ]

        # Find potential directives
        matches = re.finditer(r"@(\w+)", line)

        for match in matches:
            directive_name = match.group(1)
            start_pos = match.start()

            # Skip CSS rules
            if directive_name in css_rules:
                continue

            # Skip if it looks like an email address (has alphanumeric before @)
            if start_pos > 0:
                char_before = line[start_pos - 1]
                if char_before.isalnum() or char_before in "._-":
                    continue

            # Skip if it's inside quotes (CSS content, placeholders, etc.)
            quote_before = line[:start_pos].count('"') % 2
            single_quote_before = line[:start_pos].count("'") % 2
            if quote_before == 1 or single_quote_before == 1:
                continue

            # This looks like a real directive
            return True

        return False

    def has_echo_statements(self, line: str) -> bool:
        """Check if line has echo statements {{ }}."""
        return "{{" in line and "}}" in line

    def has_raw_echo_statements(self, line: str) -> bool:
        """Check if line has raw echo statements {!! !!}."""
        return "{!!" in line and "!!}" in line

    def compile_directive_line(self, line: str, directives) -> List[str]:
        """Compile line containing directives."""
        compiled_lines = []

        # Find all directives in the line
        directive_pattern = r"@(\w+)(?:\((.*?)\))?"
        matches = re.finditer(directive_pattern, line)

        for match in matches:
            directive_name = match.group(1)
            directive_args = match.group(2) or ""

            if directives.has(directive_name):
                handler = directives.get(directive_name)
                compiled_directive = handler(directive_args)

                # Handle indentation for control structures
                if directive_name in [
                    "if",
                    "elseif",
                    "else",
                    "for",
                    "foreach",
                    "while",
                    "isset",
                    "empty",
                ]:
                    compiled_lines.append("    " * self.indent_level + compiled_directive)
                    if directive_name not in [
                        "else",
                        "elseif",
                    ]:  # Don't increase indent for else/elseif
                        self.indent_level += 1
                elif directive_name in [
                    "endif",
                    "endfor",
                    "endforeach",
                    "endwhile",
                    "endisset",
                    "endempty",
                ]:
                    self.indent_level = max(0, self.indent_level - 1)
                    compiled_lines.append("    " * self.indent_level + compiled_directive)
                elif directive_name in ["comment"]:
                    # Skip comment directives - don't output anything
                    pass
                elif directive_name in ["endcomment"]:
                    # Skip end comment directives
                    pass
                else:
                    compiled_lines.append("    " * self.indent_level + compiled_directive)
            else:
                # Throw exception for invalid directives
                raise ValueError(f"Unknown directive: @{directive_name}")

        # Handle any remaining content in the line after removing directives
        remaining_content = re.sub(directive_pattern, "", line).strip()
        if remaining_content:
            if self.has_echo_statements(remaining_content):
                compiled_lines.extend(self.compile_echo_line(remaining_content))
            elif self.has_raw_echo_statements(remaining_content):
                compiled_lines.extend(self.compile_raw_echo_line(remaining_content))
            else:
                # Only add content if we're not in a comment block
                escaped_content = remaining_content.replace('"', '\\"').replace(
                    "'", "\\'"
                )
                compiled_lines.append(
                    "    " * self.indent_level + f"__output__ += '{escaped_content}\\n'"
                )

        return compiled_lines

    def compile_echo_line(self, line: str) -> List[str]:
        """Compile line with echo statements {{ }}."""
        compiled_lines = []

        # Split line by echo statements
        parts = re.split(r"\{\{(.*?)\}\}", line)

        output_parts = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                # Regular content
                if part:
                    escaped_part = part.replace('"', '\\"').replace("'", "\\'")
                    output_parts.append(f"'{escaped_part}'")
            else:
                # Echo statement
                expression = part.strip()
                # Handle Python reserved keywords
                if expression in ["class", "def", "for", "if", "else", "import", "from"]:
                    expression = f"data['{expression}']"
                # Check if expression uses raw() function - don't escape it
                if expression.startswith("raw("):
                    output_parts.append(f"str({expression})")
                else:
                    output_parts.append(f"escape({expression})")

        if output_parts:
            output_expr = " + ".join(output_parts)
            compiled_lines.append(
                "    " * self.indent_level + f"__output__ += {output_expr}"
            )

        return compiled_lines

    def compile_raw_echo_line(self, line: str) -> List[str]:
        """Compile line with raw echo statements {!! !!}."""
        compiled_lines = []

        # Split line by raw echo statements
        parts = re.split(r"\{!!(.*?)!!\}", line)

        output_parts = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                # Regular content
                if part:
                    escaped_part = part.replace('"', '\\"').replace("'", "\\'")
                    output_parts.append(f"'{escaped_part}'")
            else:
                # Raw echo statement
                expression = part.strip()
                # Handle Python reserved keywords
                if expression in ["class", "def", "for", "if", "else", "import", "from"]:
                    expression = f"data['{expression}']"
                output_parts.append(f"str({expression})")

        if output_parts:
            output_expr = " + ".join(output_parts)
            compiled_lines.append(
                "    " * self.indent_level + f"__output__ += {output_expr}"
            )

        return compiled_lines

    def compile_mixed_echo_line(self, line: str) -> List[str]:
        """Compile line with both echo and raw echo statements."""
        compiled_lines = []

        # First replace raw echo statements, then regular echo statements
        result_line = line

        # Process raw echo statements first
        raw_echo_pattern = r"\{!!(.*?)!!\}"
        raw_matches = re.finditer(raw_echo_pattern, result_line)
        raw_replacements = []

        for match in raw_matches:
            expression = match.group(1).strip()
            # Handle Python reserved keywords
            if expression in ["class", "def", "for", "if", "else", "import", "from"]:
                expression = f"data['{expression}']"
            raw_replacements.append(
                (match.group(0), f"{{RAW_ECHO_{len(raw_replacements)}}}")
            )

        # Replace raw echo with placeholders
        for original, placeholder in raw_replacements:
            result_line = result_line.replace(original, placeholder)

        # Now process regular echo statements
        echo_pattern = r"\{\{(.*?)\}\}"
        parts = re.split(echo_pattern, result_line)

        output_parts = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                # Regular content (may contain raw echo placeholders)
                if part:
                    # Restore raw echo expressions
                    for j, (original, placeholder) in enumerate(raw_replacements):
                        if placeholder in part:
                            expression = (
                                re.search(r"\{!!(.*?)!!\}", original).group(1).strip()
                            )
                            if expression in [
                                "class",
                                "def",
                                "for",
                                "if",
                                "else",
                                "import",
                                "from",
                            ]:
                                expression = f"data['{expression}']"
                            part = part.replace(placeholder, f"' + str({expression}) + '")

                    if part:
                        escaped_part = part.replace('"', '\\"').replace("'", "\\'")
                        output_parts.append(f"'{escaped_part}'")
            else:
                # Regular echo statement
                expression = part.strip()
                # Handle Python reserved keywords
                if expression in ["class", "def", "for", "if", "else", "import", "from"]:
                    expression = f"data['{expression}']"
                output_parts.append(f"escape({expression})")

        if output_parts:
            output_expr = " + ".join(output_parts)
            # Clean up any double concatenations
            output_expr = (
                output_expr.replace("+ '' +", "+")
                .replace("'' + ", "")
                .replace(" + ''", "")
            )
            compiled_lines.append(
                "    " * self.indent_level + f"__output__ += {output_expr}"
            )

        return compiled_lines

    def extract_sections(self, template: str) -> Dict[str, str]:
        """Extract sections from template."""
        sections = {}

        # Find all sections
        section_pattern = r'@section\([\'"](\w+)[\'"]\)(.*?)@endsection'
        matches = re.finditer(section_pattern, template, re.DOTALL)

        for match in matches:
            section_name = match.group(1)
            section_content = match.group(2).strip()
            sections[section_name] = section_content

        return sections

    def extract_extends(self, template: str) -> str:
        """Extract extends directive from template."""
        extends_pattern = r'@extends\([\'"]([^\'"]+)[\'"]\)'
        match = re.search(extends_pattern, template)

        if match:
            return match.group(1)

        return None

    def process_includes(self, template: str) -> str:
        """Process include directives."""
        # This would need integration with the view engine to load included templates
        include_pattern = r'@include\([\'"]([^\'"]+)[\'"]\)'

        def replace_include(match):
            include_view = match.group(1)
            # In a real implementation, this would load and compile the included view
            return f"<!-- Include: {include_view} -->"

        return re.sub(include_pattern, replace_include, template)

    def process_yields(self, template: str, sections: Dict[str, str]) -> str:
        """Process yield directives with section content."""

        def replace_yield(match):
            section_name = match.group(1)
            default_content = match.group(2) if match.lastindex > 1 else ""

            if section_name in sections:
                return sections[section_name]
            else:
                return default_content

        yield_pattern = r'@yield\([\'"](\w+)[\'"](?:,\s*[\'"]([^\'"]*)[\'"])?\)'
        return re.sub(yield_pattern, replace_yield, template)

    def compile_control_structure(self, directive: str, expression: str) -> str:
        """Compile control structure directive."""
        if directive == "if":
            return f"if {expression}:"
        elif directive == "elseif":
            return f"elif {expression}:"
        elif directive == "else":
            return "else:"
        elif directive == "endif":
            return "pass"
        elif directive == "for":
            return f"for {expression}:"
        elif directive == "endfor":
            return "pass"
        elif directive == "foreach":
            return f"for {expression}:"
        elif directive == "endforeach":
            return "pass"
        elif directive == "while":
            return f"while {expression}:"
        elif directive == "endwhile":
            return "pass"
        else:
            return f"# {directive} {expression}"

    def escape_string(self, value: str) -> str:
        """Escape string for Python code."""
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")

    def reset(self):
        """Reset compiler state."""
        self.indent_level = 0
        self.sections = {}
        self.extends_template = None
