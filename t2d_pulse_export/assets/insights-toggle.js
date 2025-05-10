// JavaScript to toggle market insights panels

document.addEventListener('DOMContentLoaded', function() {
    // Add click event listeners to all insight buttons
    document.addEventListener('click', function(e) {
        // Check if the clicked element has the insights-button class
        if (e.target && (
            e.target.classList.contains('insights-button') || 
            (e.target.parentElement && e.target.parentElement.classList.contains('insights-button')) ||
            (e.target.parentElement && e.target.parentElement.parentElement && 
             e.target.parentElement.parentElement.classList.contains('insights-button'))
        )) {
            // Find the button element (may be the clicked element or a parent)
            let button = e.target;
            while (button && !button.classList.contains('insights-button')) {
                button = button.parentElement;
            }
            
            if (!button) return;
            
            // Get the button ID and find the corresponding content
            const buttonId = button.id;
            const contentId = buttonId.replace('-button', '-content');
            const content = document.getElementById(contentId);
            
            if (content) {
                // Toggle the display
                if (content.style.display === 'none') {
                    content.style.display = 'block';
                    
                    // Change the icon to up arrow
                    const icon = button.querySelector('i');
                    if (icon) {
                        icon.className = 'fas fa-chevron-up';
                    }
                } else {
                    content.style.display = 'none';
                    
                    // Change the icon to down arrow
                    const icon = button.querySelector('i');
                    if (icon) {
                        icon.className = 'fas fa-chevron-down';
                    }
                }
            }
        }
    });
});