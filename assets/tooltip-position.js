// Tool tip positioning script for T2D Pulse dashboard
document.addEventListener('DOMContentLoaded', function() {
  // Function to position tooltip content
  function setupTooltips() {
    const tooltipWrappers = document.querySelectorAll('.tooltip-wrapper');
    
    tooltipWrappers.forEach(wrapper => {
      const tooltipIcon = wrapper.querySelector('.tooltip-icon');
      const tooltipContent = wrapper.querySelector('.tooltip-content');
      
      if (tooltipIcon && tooltipContent) {
        // Add event listeners for both mouseenter and focus
        tooltipIcon.addEventListener('mouseenter', positionTooltip);
        tooltipIcon.addEventListener('focus', positionTooltip);
        
        function positionTooltip() {
          // Get position of the icon
          const iconRect = tooltipIcon.getBoundingClientRect();
          
          // Calculate center position above the icon
          const top = iconRect.top - 10 - tooltipContent.offsetHeight;
          const left = iconRect.left + (iconRect.width / 2) - (tooltipContent.offsetWidth / 2);
          
          // Apply positioning
          tooltipContent.style.top = `${Math.max(10, top)}px`; // Ensure it's not above viewport
          tooltipContent.style.left = `${Math.max(10, Math.min(left, window.innerWidth - tooltipContent.offsetWidth - 10))}px`;
        }
      }
    });
  }
  
  // Call setup initially and after a short delay to ensure DOM is fully loaded
  setTimeout(setupTooltips, 1000);
  
  // Watch for any dynamic changes that might add new tooltips
  const observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
      if (mutation.addedNodes && mutation.addedNodes.length > 0) {
        setupTooltips();
      }
    });
  });
  
  // Start observing the document for changes
  observer.observe(document.body, { 
    childList: true,
    subtree: true
  });
});