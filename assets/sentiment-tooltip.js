// Simple tooltip functionality
document.addEventListener('DOMContentLoaded', function() {
  // Get the tooltip and info icon elements
  let tooltipVisible = false;
  
  // Wait a bit to ensure all elements are loaded
  setTimeout(function() {
    // Create a direct function to toggle tooltip
    window.toggleSentimentTooltip = function() {
      const tooltip = document.getElementById('sentiment-info-tooltip');
      if (!tooltip) return;
      
      tooltipVisible = !tooltipVisible;
      tooltip.style.display = tooltipVisible ? 'block' : 'none';
      console.log('Tooltip toggled:', tooltipVisible ? 'visible' : 'hidden');
    };
    
    // Add click handler to info icon
    const infoIcon = document.getElementById('sentiment-info-icon');
    if (infoIcon) {
      infoIcon.addEventListener('click', function(e) {
        console.log('Info icon clicked directly');
        window.toggleSentimentTooltip();
        e.stopPropagation();
      });
    }
    
    // Close tooltip when clicking elsewhere
    document.addEventListener('click', function(e) {
      const tooltip = document.getElementById('sentiment-info-tooltip');
      if (!tooltip) return;
      
      if (tooltipVisible && 
          e.target.id !== 'sentiment-info-icon' && 
          !tooltip.contains(e.target)) {
        tooltipVisible = false;
        tooltip.style.display = 'none';
        console.log('Tooltip closed by outside click');
      }
    });
    
    // Initialize tooltip state
    const tooltip = document.getElementById('sentiment-info-tooltip');
    if (tooltip) {
      tooltip.style.display = 'none';
      console.log('Tooltip initialized to hidden state');
    }
  }, 300);
});