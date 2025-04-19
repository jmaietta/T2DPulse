// Wait for the page to fully load
window.addEventListener('DOMContentLoaded', (event) => {
  // Add click event listener once the DOM is ready
  document.addEventListener('click', function(e) {
    // Find the sentiment info icon
    const infoIcon = document.getElementById('sentiment-info-icon');
    const tooltip = document.getElementById('sentiment-info-tooltip');
    
    if (infoIcon && tooltip) {
      // If the clicked element is the info icon, toggle the tooltip
      if (e.target === infoIcon) {
        // Toggle display between 'none' and 'block'
        if (tooltip.style.display === 'none') {
          tooltip.style.display = 'block';
        } else {
          tooltip.style.display = 'none';
        }
        e.stopPropagation(); // Prevent event from bubbling to document
      } 
      // If the click is not on the tooltip or the icon, hide the tooltip
      else if (!tooltip.contains(e.target) && tooltip.style.display === 'block') {
        tooltip.style.display = 'none';
      }
    }
  });
});