from jinja2 import Template, TemplateError
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class TemplateRenderer:
    @staticmethod
    def render(template_string: str, payload: Dict) -> Optional[str]:
        try:
            template = Template(template_string)
            rendered = template.render(**payload)
            return rendered
        except TemplateError as e:
            logger.error(f"Template rendering error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during template rendering: {e}")
            return None
    
    @staticmethod
    def render_subject_and_body(subject_template: Optional[str], body_template: str, payload: Dict) -> Dict[str, Optional[str]]:
        result = {'subject': None, 'body': None}
        if subject_template:
            result['subject'] = TemplateRenderer.render(subject_template, payload)
        result['body'] = TemplateRenderer.render(body_template, payload)
        return result