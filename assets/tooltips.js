/* Custom tooltip handlers for T2D Pulse Dashboard */

document.addEventListener('DOMContentLoaded', function() {
    // Wait for the document to be fully loaded
    setTimeout(function() {
        // Get the info icon and the tooltip
        const infoIcon = document.getElementById('info-icon');
        const tooltip = document.getElementById('sentiment-tooltip');
        
        if (infoIcon && tooltip) {
            // Show the tooltip when hovering over the info icon
            infoIcon.addEventListener('mouseenter', function() {
                tooltip.style.display = 'block';
            });
            
            // Hide the tooltip when the mouse leaves the info icon
            infoIcon.addEventListener('mouseleave', function() {
                tooltip.style.display = 'none';
            });
            
            // Also hide when clicking elsewhere on the page
            document.addEventListener('click', function(event) {
                if (event.target !== infoIcon && !tooltip.contains(event.target)) {
                    tooltip.style.display = 'none';
                }
            });
        }
    }, 1000); // Give time for Dash to render the components
});
