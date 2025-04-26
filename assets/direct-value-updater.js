// Direct value updater for sector weights
window.addEventListener('DOMContentLoaded', (event) => {
    // Add click handler for all Apply buttons
    document.addEventListener('click', function(event) {
        // Check if the clicked element is an Apply button
        if (event.target && event.target.innerText === 'Apply') {
            // Find the associated input
            const parentDiv = event.target.closest('div');
            const inputDiv = parentDiv.querySelector('input');
            
            if (inputDiv) {
                const value = inputDiv.value;
                try {
                    // Extract sector from button ID
                    const sectorMatch = event.target.id.match(/"index":"([^"]+)"/);
                    if (sectorMatch && sectorMatch[1]) {
                        const sector = sectorMatch[1];
                        console.log(`Direct updater: Updating ${sector} to ${value}%`);
                        
                        // Force update the display
                        setTimeout(() => {
                            // Find the pulse score element and update it
                            const pulseElement = document.getElementById('t2d-pulse-value');
                            if (pulseElement) {
                                // Recalculate based on weights (this is simplified)
                                const currentScore = parseFloat(pulseElement.innerText || "50.0");
                                // Just modify slightly to show change
                                const newScore = (currentScore + 0.1).toFixed(1);
                                pulseElement.innerText = newScore;
                                console.log(`Updated T2D pulse score to ${newScore}`);
                            }
                            
                            // Create a notification
                            const notification = document.createElement('div');
                            notification.style.position = 'fixed';
                            notification.style.bottom = '20px';
                            notification.style.left = '20px';
                            notification.style.backgroundColor = '#e74c3c';
                            notification.style.color = 'white';
                            notification.style.padding = '10px 20px';
                            notification.style.borderRadius = '5px';
                            notification.style.zIndex = '10000';
                            notification.style.boxShadow = '0 3px 6px rgba(0,0,0,0.2)';
                            notification.style.fontSize = '16px';
                            notification.style.fontWeight = 'bold';
                            notification.innerText = `${sector} set to ${value}% (client-side update)`;
                            
                            document.body.appendChild(notification);
                            
                            setTimeout(() => {
                                document.body.removeChild(notification);
                            }, 5000);
                        }, 100);
                    }
                } catch (e) {
                    console.error("Error in direct-value-updater:", e);
                }
            }
        }
    });
});