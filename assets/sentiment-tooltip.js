// Wait for the page to fully load
window.addEventListener('DOMContentLoaded', (event) => {
  // Ensure we initialize the tooltip as hidden
  setTimeout(function() {
    const tooltip = document.getElementById('sentiment-info-tooltip');
    if (tooltip) {
      tooltip.style.display = 'none';
    }
  }, 500); // Short delay to ensure elements are rendered
  
  // Add click event listener to the info icon specifically
  const setupIconListener = () => {
    const infoIcon = document.getElementById('sentiment-info-icon');
    if (infoIcon) {
      infoIcon.addEventListener('click', function(e) {
        const tooltip = document.getElementById('sentiment-info-tooltip');
        if (tooltip) {
          console.log('Info icon clicked');
          // Toggle display
          if (tooltip.style.display === 'none' || !tooltip.style.display) {
            tooltip.style.display = 'block';
          } else {
            tooltip.style.display = 'none';
          }
          e.stopPropagation(); // Prevent event from bubbling
        }
      });
    }
  };
  
  // Try to set up the listener immediately
  setupIconListener();
  
  // And also after a short delay to ensure the DOM is fully loaded
  setTimeout(setupIconListener, 1000);
  
  // Close tooltip when clicking anywhere else
  document.addEventListener('click', function(e) {
    const tooltip = document.getElementById('sentiment-info-tooltip');
    const infoIcon = document.getElementById('sentiment-info-icon');
    
    if (tooltip && tooltip.style.display === 'block') {
      // If the click is not on the tooltip or the info icon, hide the tooltip
      if (!tooltip.contains(e.target) && e.target !== infoIcon) {
        tooltip.style.display = 'none';
      }
    }
  });
  
  // Add close button to tooltip
  setTimeout(function() {
    const tooltip = document.getElementById('sentiment-info-tooltip');
    if (tooltip) {
      // Add a close button if it doesn't exist
      if (!document.getElementById('tooltip-close-btn')) {
        const closeBtn = document.createElement('div');
        closeBtn.id = 'tooltip-close-btn';
        closeBtn.innerHTML = '&times;'; // × symbol
        closeBtn.style.position = 'absolute';
        closeBtn.style.top = '5px';
        closeBtn.style.right = '10px';
        closeBtn.style.cursor = 'pointer';
        closeBtn.style.fontSize = '20px';
        closeBtn.style.fontWeight = 'bold';
        closeBtn.style.color = '#555';
        
        closeBtn.addEventListener('click', function() {
          tooltip.style.display = 'none';
        });
        
        tooltip.style.position = 'relative'; // Ensure the tooltip is positioned relatively
        tooltip.insertBefore(closeBtn, tooltip.firstChild);
      }
    }
  }, 800);
});