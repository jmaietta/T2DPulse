// Add client-side feedback for weight updates
window.addEventListener('DOMContentLoaded', (event) => {
    const feedbackDiv = document.createElement('div');
    feedbackDiv.id = 'weight-update-feedback';
    feedbackDiv.style.position = 'fixed';
    feedbackDiv.style.top = '20px';
    feedbackDiv.style.right = '20px';
    feedbackDiv.style.padding = '12px 16px';
    feedbackDiv.style.backgroundColor = '#4CAF50';
    feedbackDiv.style.color = 'white';
    feedbackDiv.style.borderRadius = '4px';
    feedbackDiv.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
    feedbackDiv.style.zIndex = '1000';
    feedbackDiv.style.opacity = '0';
    feedbackDiv.style.transition = 'opacity 0.3s ease';
    feedbackDiv.style.fontSize = '16px';
    feedbackDiv.style.fontWeight = 'bold';
    document.body.appendChild(feedbackDiv);

    // Add click handler for all Apply buttons (immediate feedback)
    document.addEventListener('click', function(event) {
        // Check if the clicked element is an Apply button
        if (event.target && event.target.innerText === 'Apply') {
            // Find the associated input
            const parentDiv = event.target.closest('div');
            const inputDiv = parentDiv.querySelector('input');
            
            if (inputDiv) {
                const value = inputDiv.value;
                
                try {
                    const sector = event.target.id.match(/"index":"([^"]+)"/)[1];
                    
                    // Show feedback immediately on click
                    feedbackDiv.innerText = `Updated ${sector} weight to ${value}%`;
                    feedbackDiv.style.opacity = '1';
                    
                    // Clear feedback after 3 seconds
                    setTimeout(() => {
                        feedbackDiv.style.opacity = '0';
                    }, 3000);
                } catch (e) {
                    console.error("Error parsing sector from ID:", e);
                }
            }
        }
    });

    // Also observe the notification div for changes
    setTimeout(() => {
        const notificationDiv = document.getElementById('weight-update-notification');
        if (notificationDiv) {
            const observer = new MutationObserver((mutationsList, observer) => {
                for(const mutation of mutationsList) {
                    if (mutation.type === 'childList' && notificationDiv.innerText.trim() !== '') {
                        const updatedText = notificationDiv.innerText.trim();

                        // Show popup with the same message
                        feedbackDiv.innerText = updatedText;
                        feedbackDiv.style.opacity = '1';

                        // Clear after 3 seconds
                        setTimeout(() => {
                            feedbackDiv.style.opacity = '0';
                        }, 3000);
                    }
                }
            });

            observer.observe(notificationDiv, { childList: true });
            console.log("Observer attached to notification div");
        } else {
            console.log("Notification div not found");
        }
    }, 2000); // Wait for the div to be available
});