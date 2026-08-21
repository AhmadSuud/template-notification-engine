"""
Template rendering module using Jinja2
Renders notification templates with payload variables
"""
from jinja2 import Template, TemplateError
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class TemplateRenderer:
    """Jinja2 template renderer for notification messages"""
    
    @staticmethod
    def render(template_string: str, payload: Dict) -> Optional[str]:
        """
        Render a template string with payload variables
        
        Args:
            template_string: Template string with {{variable}} placeholders
            payload: Dictionary containing variable values
            
        Returns:
            Rendered string or None if rendering fails
        """
        try:
            template = Template(template_string)
            rendered = template.render(**payload)
            logger.debug(f"Template rendered successfully")
            return rendered
            
        except TemplateError as e:
            logger.error(f"Template rendering error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during template rendering: {e}")
            return None
    
    @staticmethod
    def render_subject_and_body(subject_template: Optional[str], body_template: str, payload: Dict) -> Dict[str, Optional[str]]:
        """
        Render both subject and body templates
        
        Args:
            subject_template: Subject template string (can be None for channels without subject)
            body_template: Body template string
            payload: Dictionary containing variable values
            
        Returns:
            Dictionary with 'subject' and 'body' keys
        """
        result = {
            'subject': None,
            'body': None
        }
        
        # Render subject if provided
        if subject_template:
            result['subject'] = TemplateRenderer.render(subject_template, payload)
        
        # Render body
        result['body'] = TemplateRenderer.render(body_template, payload)
        
        return result
    
    @staticmethod
    def validate_template(template_string: str, required_variables: list) -> tuple[bool, Optional[str]]:
        """
        Validate that a template contains all required variables
        
        Args:
            template_string: Template string to validate
            required_variables: List of required variable names
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            template = Template(template_string)
            
            # Get all variable names from template
            from jinja2.meta import find_undeclared_variables
            from jinja2 import Environment
            
            env = Environment()
            parsed = env.parse(template_string)
            template_vars = find_undeclared_variables(parsed)
            
            # Check if all required variables are present
            missing_vars = set(required_variables) - template_vars
            
            if missing_vars:
                error_msg = f"Template missing required variables: {', '.join(missing_vars)}"
                logger.warning(error_msg)
                return False, error_msg
            
            return True, None
            
        except TemplateError as e:
            error_msg = f"Invalid template syntax: {e}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Template validation error: {e}"
            logger.error(error_msg)
            return False, error_msg
