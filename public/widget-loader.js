/**
 * Chat Widget Loader
 * 
 * Embed this widget on any website using:
 * <script src="https://bcca.ai/flask/widget-loader.js" 
 *         data-mode="your-mode-id" 
 *         data-theme="#82002d"
 *         data-position="bottom-right"></script>
 */

(function() {
  'use strict';
  
  // Get the current script tag to read data attributes
  const widgetScript = document.currentScript || (function() {
    const scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();
  
  // Configuration from data attributes
  const config = {
    mode: widgetScript.getAttribute('data-mode') || '',
    theme: widgetScript.getAttribute('data-theme') || '#82002d',
    position: widgetScript.getAttribute('data-position') || 'bottom-right', // bottom-right, bottom-left, top-right, top-left
    baseUrl: widgetScript.getAttribute('data-base-url') || window.location.origin
  };
  
  // Validate required config
  if (!config.mode) {
    console.error('Chat Widget Error: data-mode attribute is required');
    return;
  }
  
  // Position styles
  const positions = {
    'bottom-right': 'bottom: 20px; right: 20px;',
    'bottom-left': 'bottom: 20px; left: 20px;',
    'top-right': 'top: 20px; right: 20px;',
    'top-left': 'top: 20px; left: 20px;'
  };
  
  const positionStyle = positions[config.position] || positions['bottom-right'];
  
  // Create iframe container
  const createWidget = function() {
    // Create container div
    const container = document.createElement('div');
    container.id = 'chat-widget-container';
    container.style.cssText = `
      position: fixed;
      ${positionStyle}
      z-index: 999999;
      width: 420px;
      height: 600px;
      max-width: calc(100vw - 40px);
      max-height: calc(100vh - 100px);
    `;
    
    // Create iframe
    const iframe = document.createElement('iframe');
    const widgetUrl = `${config.baseUrl}/chat-widget.html?mode=${encodeURIComponent(config.mode)}&theme=${encodeURIComponent(config.theme)}`;
    
    iframe.src = widgetUrl;
    iframe.style.cssText = `
      width: 100%;
      height: 100%;
      border: none;
      border-radius: 24px;
      box-shadow: none;
      background: transparent;
    `;
    iframe.allow = 'clipboard-write';
    iframe.title = 'Chat Widget';
    
    // Responsive handling for mobile
    const handleResize = function() {
      if (window.innerWidth <= 480) {
        container.style.cssText = `
          position: fixed;
          bottom: 0;
          left: 0;
          right: 0;
          top: 0;
          z-index: 999999;
          width: 100%;
          height: 100%;
          max-width: 100%;
          max-height: 100%;
        `;
        iframe.style.borderRadius = '0';
      } else {
        container.style.cssText = `
          position: fixed;
          ${positionStyle}
          z-index: 999999;
          width: 420px;
          height: 600px;
          max-width: calc(100vw - 40px);
          max-height: calc(100vh - 100px);
        `;
        iframe.style.borderRadius = '24px';
      }
    };
    
    window.addEventListener('resize', handleResize);
    handleResize();
    
    container.appendChild(iframe);
    document.body.appendChild(container);
    
    console.log('Chat Widget loaded successfully');
  };
  
  // Wait for DOM to be ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', createWidget);
  } else {
    createWidget();
  }
  
})();

