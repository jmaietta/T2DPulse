// JavaScript to highlight weight input fields when values change
window.addEventListener('load', function() {
    // Add a small delay to ensure all elements are loaded
    setTimeout(function() {
        // Target all weight inputs by their pattern
        const weightInputs = document.querySelectorAll('[id^="weight-input"]');
        
        function highlightInput(inputDiv) {
            // Get the actual input field (the first input inside the div)
            const input = inputDiv.querySelector('input');
            if (!input) return;
            
            // Add highlighting class
            input.classList.add('weight-value-changed');
            
            // Remove the class after animation completes
            setTimeout(function() {
                input.classList.remove('weight-value-changed');
            }, 1500);
        }
        
        // Update this function to use MutationObserver to detect when weight values change
        function observeWeightChanges() {
            // Get all input containers
            const inputContainers = document.querySelectorAll('[id^="input-container"]');
            
            inputContainers.forEach(container => {
                // Create a new observer instance for each input
                const observer = new MutationObserver(function(mutations) {
                    mutations.forEach(function(mutation) {
                        if (mutation.type === 'attributes' && mutation.attributeName === 'value') {
                            highlightInput(container);
                        }
                    });
                });
                
                // Configuration of the observer
                const config = { 
                    attributes: true,
                    childList: true,
                    subtree: true,
                    attributeFilter: ['value']
                };
                
                // Start observing
                observer.observe(container, config);
            });
        }
        
        // Initialize the observers
        observeWeightChanges();
        
        // Also setup to watch for new weight inputs that might be added dynamically
        const sectorContainer = document.querySelector('.sector-cards-grid');
        if (sectorContainer) {
            const containerObserver = new MutationObserver(function(mutations) {
                observeWeightChanges();
            });
            
            containerObserver.observe(sectorContainer, { 
                childList: true,
                subtree: true 
            });
        }
    }, 1000);
});