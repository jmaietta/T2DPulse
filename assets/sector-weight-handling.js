// Handle Enter Key press in sector weight inputs
window.addEventListener('DOMContentLoaded', function() {
    // Periodically check for inputs with data-enter-submit attribute
    // This handles dynamically added inputs
    setInterval(function() {
        // Find all inputs with the data-enter-submit attribute
        var inputs = document.querySelectorAll('input[data-enter-submit="true"]');
        
        // Add keydown event listeners to each input
        inputs.forEach(function(input) {
            // Skip inputs that already have the handler
            if (input.dataset.enterHandlerAttached === "true") {
                return;
            }
            
            // Add event listener for keydown
            input.addEventListener('keydown', function(event) {
                // If Enter key is pressed
                if (event.key === 'Enter') {
                    event.preventDefault();
                    
                    // Get the sector from the data attribute
                    var sector = input.dataset.sector;
                    
                    // Find and click the corresponding hidden button
                    var hiddenButton = document.querySelector('button[id*="hidden-submit"][id*="' + sector + '"]');
                    if (hiddenButton) {
                        hiddenButton.click();
                    } else {
                        // Fallback: find and click the Apply button
                        var applyButton = document.querySelector('button[id*="apply-weight"][id*="' + sector + '"]');
                        if (applyButton) {
                            applyButton.click();
                        }
                    }
                }
            });
            
            // Mark this input as handled
            input.dataset.enterHandlerAttached = "true";
        });
    }, 500); // Check every half second
});