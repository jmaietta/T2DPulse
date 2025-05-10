// JavaScript for tooltip functionality

document.addEventListener('DOMContentLoaded', function() {
    // Get the info icon and tooltip elements
    const infoIcon = document.getElementById('info-icon');
    const sentimentTooltip = document.getElementById('sentiment-tooltip');
    
    if (infoIcon && sentimentTooltip) {
        // Show tooltip on hover
        infoIcon.addEventListener('mouseenter', function() {
            sentimentTooltip.style.display = 'block';
        });
        
        // Hide tooltip when mouse leaves
        infoIcon.addEventListener('mouseleave', function() {
            sentimentTooltip.style.display = 'none';
        });
        
        // Also hide tooltip when clicked elsewhere on the page
        document.addEventListener('click', function(event) {
            if (event.target !== infoIcon && !sentimentTooltip.contains(event.target)) {
                sentimentTooltip.style.display = 'none';
            }
        });
    }
});
