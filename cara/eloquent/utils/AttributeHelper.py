"""
AttributeHelper - Utility for attribute operations
"""

class AttributeHelper:
    """Helper for attribute manipulation operations."""
    
    @staticmethod
    def clean_attribute_name(name: str) -> str:
        """Clean attribute name."""
        return name.strip().lower()
    
    @staticmethod
    def is_fillable(attribute: str, fillable: list, guarded: list) -> bool:
        """Check if attribute is fillable."""
        if "*" in guarded:
            return False
        if attribute in guarded:
            return False
        if "*" in fillable:
            return True
        return attribute in fillable
    
    @staticmethod
    def get_accessor_method_name(attribute: str) -> str:
        """Get accessor method name."""
        return f"get_{attribute}_attribute"
    
    @staticmethod
    def get_mutator_method_name(attribute: str) -> str:
        """Get mutator method name."""
        return f"set_{attribute}_attribute"
    
    @staticmethod
    def is_hidden(attribute: str, hidden: list, visible: list = None) -> bool:
        """Check if attribute should be hidden."""
        if visible and attribute not in visible:
            return True
        return attribute in hidden

