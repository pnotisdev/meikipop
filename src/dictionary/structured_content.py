from typing import Any, List, Dict

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_node(node: Any) -> str:
    """Recursively render a structured content node to HTML."""
    if isinstance(node, str):
        return escape_html(node)
    
    if isinstance(node, list):
        return "".join(render_node(child) for child in node)
        
    if isinstance(node, dict):
        # Handle "content" being list or string
        content_obj = node.get('content')
        inner_html = ""
        if content_obj:
            inner_html = render_node(content_obj)
            
        tag = node.get('tag')
        if not tag:
            return inner_html
            
        # Map tags to HTML
        # Supported subset by Qt: span, div, p, br, table, tr, td, ul, ol, li, b, i, u, s, img, sub, sup, etc.
        if tag == 'br':
            return "<br>"
        elif tag in ('span', 'div', 'p', 'b', 'i', 'u', 's', 'sub', 'sup', 'strong', 'em', 'small', 'big'):
            # Simple wrapper tags
            # We can extract minimal style if needed, but for now just tag passing.
            style_str = ""
            data = node.get('data')
            if data and isinstance(data, dict):
                # Basic style mapping could go here
                pass
            return f"<{tag} {style_str}>{inner_html}</{tag}>"
        elif tag == 'ul':
            return f"<ul style='margin: 0; padding-left: 20px;'>{inner_html}</ul>"
        elif tag == 'ol':
            return f"<ol style='margin: 0; padding-left: 20px;'>{inner_html}</ol>"
        elif tag == 'li':
            return f"<li>{inner_html}</li>"
        elif tag == 'table':
            return f"<table border='1' cellspacing='0' cellpadding='2'>{inner_html}</table>"
        elif tag in ('tr', 'td', 'th', 'thead', 'tbody'):
            return f"<{tag}>{inner_html}</{tag}>"
        elif tag == 'ruby':
             # Return inner content, skipping ruby display to match previous logic
             return inner_html
        elif tag == 'rt':
            # Hide ruby text
            return "" 
        elif tag == 'img':
            # Placeholder for images
            return "[Image]" 
        else:
            # Fallback for unknown tags: return content
            return inner_html

    return ""

def handle_structured_content(item: Dict[str, Any]) -> List[str]:
    """
    Process dictionary item identified as 'structured-content'.
    Returns a list containing a single HTML string.
    """
    content = item.get('content')
    html_output = render_node(content)
    if html_output:
        return [html_output]
    return []
