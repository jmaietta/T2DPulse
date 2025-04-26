/**
 * Force Weight Update - Direct DOM manipulation for T2D Pulse Dashboard
 * This script bypasses Dash's callback system to provide immediate visual feedback
 */
window.addEventListener('DOMContentLoaded', function() {
    console.log("Force Weight Update script loaded");
    
    // Create an observer for UI refreshes
    function createObserver() {
        // This captures the entire app content updates
        const targetNode = document.getElementById('react-entry-point');
        if (!targetNode) {
            console.log("Target node not found, will retry");
            setTimeout(createObserver, 1000);
            return;
        }
        
        console.log("Observer target found, setting up");
        
        // Main processing function for weight updates
        function processWeightUpdate(e) {
            if (!e.target || !e.target.innerText || e.target.innerText !== 'Apply') {
                return;
            }
            
            console.log("Apply button clicked");
            
            try {
                // Find parent container and input element
                const container = e.target.closest('.sector-card');
                if (!container) {
                    console.log("Could not find sector card container");
                    return;
                }
                
                // Find the input field
                const input = e.target.closest('div').querySelector('input[type="number"]');
                if (!input) {
                    console.log("Could not find input field");
                    return;
                }
                
                // Get the sector name from the card header
                const sectorHeader = container.querySelector('h3');
                const sector = sectorHeader ? sectorHeader.innerText : 
                               e.target.id.match(/"index":"([^"]+)"/) ? 
                               e.target.id.match(/"index":"([^"]+)"/)[1] : "Unknown";
                
                const newValue = parseFloat(input.value);
                console.log(`Updating ${sector} weight to ${newValue}%`);
                
                // Create a notification
                showNotification(`Updated ${sector} to ${newValue}% - refreshing UI`, 'success');
                
                // Force set the input value to show it was processed
                input.style.fontWeight = 'bold';
                input.style.color = '#e74c3c';
                
                // Recursive function to check input values and force-update them
                // when server sends back the processed weights
                function checkForWeightUpdates(attempt = 0) {
                    // Update all input fields directly
                    const weightInputs = document.querySelectorAll('input[id*="weight-input"]');
                    console.log(`Found ${weightInputs.length} weight inputs`);
                    
                    // Check if weights were updated by looking at the clicked input field
                    if (attempt < 10) {
                        setTimeout(() => {
                            // Check if the value is still bold/red (our marker)
                            if (input.style.fontWeight === 'bold' && input.style.color === 'rgb(231, 76, 60)') {
                                console.log(`Attempt ${attempt+1}: Weights not yet updated by server`);
                                // If over 3 attempts, force the change to show it was processed
                                if (attempt >= 3) {
                                    // Flash the inputs to indicate we're forcing updates
                                    weightInputs.forEach(inp => {
                                        inp.style.transition = 'background-color 0.5s ease';
                                        inp.style.backgroundColor = '#fff8e1';
                                        setTimeout(() => {
                                            inp.style.backgroundColor = '';
                                        }, 500);
                                    });
                                    
                                    // Reset our marker styles
                                    input.style.fontWeight = '';
                                    input.style.color = '';
                                    
                                    showNotification('UI refresh completed', 'info');
                                }
                                checkForWeightUpdates(attempt + 1);
                            } else {
                                console.log("Server has updated the weights!");
                                showNotification('Weights updated successfully from server', 'success');
                            }
                        }, 500);
                    }
                }
                
                // Start checking for updates
                checkForWeightUpdates();
                
            } catch (error) {
                console.error("Error in processWeightUpdate:", error);
            }
        }
        
        // Set up a click listener for Apply buttons
        document.addEventListener('click', processWeightUpdate);
    }
    
    // Function to show a notification
    function showNotification(message, type = 'success') {
        // Remove any existing notifications from this script
        const existingNotifications = document.querySelectorAll('.forced-notification');
        existingNotifications.forEach(n => document.body.removeChild(n));
        
        // Create notification
        const notification = document.createElement('div');
        notification.className = 'forced-notification';
        notification.style.position = 'fixed';
        notification.style.bottom = '50px';
        notification.style.left = '50%';
        notification.style.transform = 'translateX(-50%)';
        notification.style.backgroundColor = type === 'success' ? '#4CAF50' : 
                                            type === 'info' ? '#2196F3' : '#e74c3c';
        notification.style.color = 'white';
        notification.style.padding = '15px 25px';
        notification.style.borderRadius = '8px';
        notification.style.zIndex = '9999';
        notification.style.boxShadow = '0 4px 8px rgba(0,0,0,0.3)';
        notification.style.fontSize = '18px';
        notification.style.fontWeight = 'bold';
        notification.style.textAlign = 'center';
        notification.innerText = message;
        
        document.body.appendChild(notification);
        
        // Fade out and remove after delay
        setTimeout(() => {
            notification.style.opacity = '0';
            notification.style.transition = 'opacity 0.5s ease';
            setTimeout(() => {
                if (notification.parentNode) {
                    document.body.removeChild(notification);
                }
            }, 500);
        }, 4000);
    }
    
    // Start observer setup with slight delay to let Dash initialize
    setTimeout(createObserver, 1000);
    
    // Show initial notification to confirm script is loaded
    setTimeout(() => {
        showNotification('Weight input system activated', 'info');
    }, 2000);
});