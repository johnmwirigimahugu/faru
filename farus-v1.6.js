// farus.js v1.6 - Frontend Zen for Green Horns
// Vision: Coding should be as easy as coloring
// Prefixes: fr- (Farus), fx- (Farus UX)

const Farus = {
    // Stores reactive state for each fx-data component instance
    _instances: new Map(), 
    lang: {},
    debug: false,
    
    /**
     * Displays a flash message in the element with fr-flash attribute.
     * @param {string} msg - The message to display.
     */
    flash: (msg) => {
        let e = document.querySelector('[fr-flash]');
        if (e) {
            e.textContent = msg;
            // Optional: Clear flash message after a delay
            setTimeout(() => e.textContent = '', 5000); 
        }
    },

    /**
     * Translates a given key based on loaded language data.
     * @param {string} k - The translation key.
     * @returns {string} The translated string or the key if not found.
     */
    t: (k) => Farus.lang[k] || k,

    /**
     * Logs messages to the console if debug mode is enabled.
     * @param {...any} args - Arguments to log.
     */
    log: (...args) => Farus.debug && console.log('[Farus]', ...args),

    /**
     * Helper for creating HTML tags.
     */
    html: {
        /**
         * Creates an HTML tag string.
         * @param {string} t - The tag name (e.g., 'div', 'span').
         * @param {Object} a - An object of attributes (e.g., { class: 'my-class', id: 'my-id' }).
         * @param {string} c - The inner HTML content.
         * @returns {string} The HTML string.
         */
        tag: (t, a = {}, c = '') => {
            let attr = Object.entries(a).map(([k, v]) => `${k}="${v}"`).join(' ');
            return `<${t} ${attr}>${c}</${t}>`;
        }
    },

    /**
     * Processes and renders a component template.
     * @param {HTMLElement} targetEl - The element where the component will be rendered.
     * @param {string} name - The name of the component.
     * @param {Object} data - Data to pass to the component.
     */
    renderComponent: (targetEl, name, data) => {
        const template = document.querySelector(`template[fr-component="${name}"]`);
        if (!template) {
            Farus.log(`Component '${name}' not found.`);
            return;
        }
        let html = template.innerHTML;
        // Simple string replacement for data. Consider a more robust templating if needed.
        for (const key in data) {
            html = html.replaceAll(`{{${key}}}`, data[key]);
        }
        targetEl.innerHTML = html;
        // Re-process any new fx-data elements within the rendered component
        targetEl.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);
    },

    /**
     * Binds reactive data to an element and its children.
     * @param {HTMLElement} el - The element with the 'fx-data' attribute.
     */
    bindFXData: (el) => {
        const initialState = JSON.parse(el.getAttribute('fx-data') || '{}');
        const persist = el.hasAttribute('fx-persist');
        // Use a unique ID for each fx-data root for better state isolation
        const instanceId = el.dataset.frInstanceId || `fr_instance_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
        el.dataset.frInstanceId = instanceId;

        let data = initialState;
        if (persist) {
            const saved = localStorage.getItem(instanceId);
            if (saved) {
                try {
                    data = { ...initialState, ...JSON.parse(saved) }; // Merge to allow new defaults
                } catch (e) {
                    Farus.log('Error parsing persisted data:', e);
                }
            }
        }

        const stateProxy = new Proxy(data, {
            set(obj, prop, val) {
                const oldVal = obj[prop];
                if (oldVal === val) return true; // Prevent unnecessary updates

                obj[prop] = val;
                Farus.updateBindings(el, stateProxy); // Update only within this instance's scope
                if (persist) localStorage.setItem(instanceId, JSON.stringify(obj));
                return true;
            }
        });

        Farus._instances.set(instanceId, stateProxy); // Store the proxy instance
        Farus.updateBindings(el, stateProxy); // Initial update
    },

    /**
     * Updates all bindings (fx-text, fx-show, fx-bind) within a specific root element.
     * This is called by the proxy setter to update only affected parts of the DOM.
     * @param {HTMLElement} root - The root element (the one with fx-data).
     * @param {Proxy} state - The proxy object for the state data.
     */
    updateBindings: (root, state) => {
        // Evaluate expression in the context of the state object (safe alternative to eval)
        const evaluate = (expression) => {
            try {
                // Using a Function constructor allows 'with' safely, but be careful with complex expressions.
                // For a more robust solution, consider a simple expression parser or a custom template literal tag.
                return new Function('state', `with(state) { return ${expression}; }`)(state);
            } catch (e) {
                Farus.log('Binding expression error:', expression, e);
                return ''; // Return empty string or a placeholder on error
            }
        };

        root.querySelectorAll('[fx-text]').forEach(el => {
            // Ensure the element is within the current root's scope
            if (root.contains(el) || el === root) {
                el.textContent = evaluate(el.getAttribute('fx-text'));
            }
        });

        root.querySelectorAll('[fx-show]').forEach(el => {
            if (root.contains(el) || el === root) {
                el.style.display = evaluate(el.getAttribute('fx-show')) ? '' : 'none';
            }
        });

        root.querySelectorAll('[fx-bind]').forEach(el => {
            if (root.contains(el) || el === root) {
                const binds = el.getAttribute('fx-bind').split(';');
                binds.forEach(b => {
                    const [attr, expr] = b.split(':').map(s => s.trim());
                    if (attr && expr) {
                        el.setAttribute(attr, evaluate(expr));
                    }
                });
            }
        });
    },

    /**
     * Handles HTTP requests triggered by fr-get, fr-post, etc.
     * @param {Event} e - The DOM event (e.g., click or submit).
     */
    handleHttpRequest: (e) => {
        const triggerEl = e.target.closest('[fr-get],[fr-post],[fr-put],[fr-delete],[fr-patch]');
        if (!triggerEl) return;

        e.preventDefault();

        const methodAttr = [...triggerEl.attributes].find(a => a.name.startsWith('fr-') && a.name !== 'fr-target' && a.name !== 'fr-swap' && a.name !== 'fr-confirm');
        if (!methodAttr) return;

        const method = methodAttr.name.split('-')[1].toUpperCase();
        const url = methodAttr.value;
        const targetSelector = triggerEl.getAttribute('fr-target');
        const targetElement = targetSelector ? document.querySelector(targetSelector) : null;
        const confirmMsg = triggerEl.getAttribute('fr-confirm');
        const swapType = triggerEl.getAttribute('fr-swap') || 'innerHTML';
        const isForm = triggerEl.tagName === 'FORM';

        if (confirmMsg && !confirm(confirmMsg)) return;

        let body = null;
        let headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Authorization': localStorage.getItem('token') || '',
            'X-CSRF-Token': document.querySelector('meta[name=csrf-token]')?.content || ''
        };

        if (method !== 'GET' && method !== 'HEAD') {
            if (isForm) {
                body = new FormData(triggerEl);
            } else {
                body = new FormData();
                [...triggerEl.attributes].forEach(a => {
                    if (a.name.startsWith('data-')) body.append(a.name.slice(5), a.value);
                });
            }
        }
        
        // Add Content-Type header for JSON if needed (e.g., if you plan to send JSON body)
        // if (triggerEl.hasAttribute('fr-json')) {
        //     headers['Content-Type'] = 'application/json';
        //     if (body instanceof FormData) {
        //         body = JSON.stringify(Object.fromEntries(body));
        //     }
        // }

        fetch(url, {
            method,
            headers,
            body: body,
            credentials: 'include' // Important for cookies/sessions
        })
        .then(response => {
            if (!response.ok) {
                Farus.log(`HTTP error! status: ${response.status} for ${url}`);
                Farus.flash(`Error: ${response.status} ${response.statusText}`);
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.text();
        })
        .then(html => {
            if (targetElement) {
                if (swapType === 'innerHTML') targetElement.innerHTML = html;
                else if (swapType === 'outerHTML') targetElement.outerHTML = html;
                // Re-initialize any new fx-data elements brought in by the swap
                if (targetElement.parentElement) {
                    targetElement.parentElement.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);
                } else if (targetElement) { // If outerHTML changed the target itself
                    document.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);
                }
            }
            Farus.log(`Request to ${url} successful.`);
        })
        .catch(e => {
            Farus.log('Fetch error:', e);
            Farus.flash(`Network error or API issue: ${e.message}`);
        });
    },

    /**
     * Handles @click event attributes.
     * @param {Event} e - The click event.
     */
    handleClickEvent: (e) => {
        const el = e.target.closest('[\\@click]');
        if (!el) return;

        const expr = el.getAttribute('@click');
        // Find the closest fx-data parent to get its state
        const fxDataParent = el.closest('[fx-data]');
        if (fxDataParent) {
            const instanceId = fxDataParent.dataset.frInstanceId;
            const state = Farus._instances.get(instanceId);
            if (state) {
                try {
                    new Function('state', `with(state) { ${expr}; }`)(state);
                } catch (err) {
                    Farus.log('Click event expression error:', expr, err);
                }
            } else {
                Farus.log('No state found for @click element:', el);
            }
        } else {
            // If @click is not within an fx-data scope, execute globally (less recommended)
            try {
                new Function(expr)();
            } catch (err) {
                Farus.log('Global click event expression error:', expr, err);
            }
        }
    },

    /**
     * Handles form validation based on 'fr-validate'.
     * @param {Event} e - The submit event.
     */
    handleFormValidation: (e) => {
        const form = e.target;
        if (!form.hasAttribute('fr-validate')) return;

        let valid = true;
        form.querySelectorAll('[required]').forEach(input => {
            if (!input.value) {
                valid = false;
                // More user-friendly validation: add/remove error classes or display messages next to inputs
                const message = input.getAttribute('fr-message') || `This field is required.`;
                Farus.flash(message); // Using flash for simplicity for now
                // Example of adding an error class: input.classList.add('is-invalid');
            } else {
                // input.classList.remove('is-invalid');
            }
        });

        if (!valid) {
            e.preventDefault(); // Stop form submission if invalid
        }
    },

    /**
     * Initializes polling for elements with 'fr-poll'.
     */
    initPolling: () => {
        document.querySelectorAll('[fr-poll]').forEach(el => {
            const url = el.getAttribute('fr-get'); // Assumes fr-poll is used with fr-get
            const interval = parseInt(el.getAttribute('fr-poll'), 10) || 3000;
            const targetSelector = el.getAttribute('fr-target');
            const targetElement = targetSelector ? document.querySelector(targetSelector) : null;

            const poll = () => {
                fetch(url, {
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Authorization': localStorage.getItem('token') || ''
                    }
                })
                .then(r => {
                    if (!r.ok) throw new Error(`HTTP error! status: ${r.status}`);
                    return r.text();
                })
                .then(html => {
                    if (targetElement) {
                        targetElement.innerHTML = html;
                        // Re-initialize fx-data for new content if needed
                        targetElement.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);
                    }
                })
                .catch(e => Farus.log('Polling error:', url, e))
                .finally(() => setTimeout(poll, interval)); // Schedule next poll after current completes
            };
            poll(); // Start initial poll
        });
    },

    /**
     * Initializes SPA routing.
     */
    initRouting: () => {
        const loadRoute = () => {
            const view = document.querySelector('[fr-view]');
            if (!view) {
                Farus.log('No [fr-view] element found for routing.');
                return;
            }
            // Fetch the content for the current path
            fetch(location.pathname, {
                headers: {
                    'X-Requested-With': 'XMLHttpRequest' // Indicate AJAX request
                }
            })
            .then(r => {
                if (!r.ok) {
                    // Handle 404 or other errors for routes gracefully
                    Farus.log(`Route load error: ${r.status}`);
                    return r.text().then(text => Promise.reject(new Error(`Failed to load route: ${r.status} ${text.substring(0, 100)}`)));
                }
                return r.text();
            })
            .then(t => {
                view.innerHTML = t;
                // Important: Re-scan for fx-data and other Farus attributes in the newly loaded content
                view.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);
                Farus.initComponents(); // Ensure components are re-processed
                // Any other dynamic features within the new content might need re-initialization here
            })
            .catch(e => {
                Farus.log('Error loading route:', e);
                view.innerHTML = `<p>Error loading content: ${e.message}</p>`; // Display error in view
            });
        };

        // Handle clicks on fr-route links
        document.addEventListener('click', e => {
            const a = e.target.closest('[fr-route]');
            if (a && a.tagName === 'A') { // Ensure it's an anchor tag
                e.preventDefault();
                const href = a.getAttribute('href');
                if (window.location.pathname !== href) { // Prevent redundant pushState
                    history.pushState({}, '', href);
                    loadRoute();
                }
            }
        });

        // Handle browser back/forward buttons
        window.addEventListener('popstate', loadRoute);

        // Load the initial route content on page load
        if (document.querySelector('[fr-view]')) {
            loadRoute();
        }
    },

    /**
     * Initializes component usage.
     */
    initComponents: () => {
        // Collect all component templates first
        const componentTemplates = {};
        document.querySelectorAll('template[fr-component]').forEach(tpl => {
            componentTemplates[tpl.getAttribute('fr-component')] = tpl.innerHTML;
        });

        // Render instances of components
        document.querySelectorAll(`[fr-use]`).forEach(target => {
            const name = target.getAttribute('fr-use');
            const html = componentTemplates[name];
            if (html) {
                let data = {};
                try {
                    data = JSON.parse(target.getAttribute('data') || '{}');
                } catch (e) {
                    Farus.log('Error parsing component data:', e);
                }
                Farus.renderComponent(target, name, data);
            }
        });
    },

    /**
     * Initializes the Farus framework.
     * This should be called once when the DOM is ready.
     */
    init: () => {
        Farus.log('Farus.js initializing...');
        // Bind all fx-data elements on initial load
        document.querySelectorAll('[fx-data]').forEach(Farus.bindFXData);

        // Attach event listeners to the document for delegation
        document.addEventListener('click', Farus.handleHttpRequest);
        document.addEventListener('submit', Farus.handleHttpRequest); // For form submissions with fr-post/put etc.
        document.addEventListener('click', Farus.handleClickEvent);
        document.addEventListener('submit', Farus.handleFormValidation);

        Farus.initPolling();
        Farus.initRouting();
        Farus.initComponents();

        Farus.log('Farus.js initialized.');
    }
};

// Initialize Farus when the DOM content is fully loaded
document.addEventListener('DOMContentLoaded', Farus.init);

// EOF
