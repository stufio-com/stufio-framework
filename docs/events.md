# EventBus System Documentation

This document explains how to use the EventBus system in our Nuxt 3 application, both in the main app and in modules.

## Table of Contents

- Overview
- Using EventBus in the Main Application
- Using EventBus in Modules
- Defining Custom Events
- Best Practices
- Troubleshooting

## Overview

The EventBus system provides a lightweight, type-safe event-based communication mechanism across your application and modules. It allows components, stores, and plugins to communicate without direct dependencies.

### Core Features

- **Centralized Event Management**: All events flow through a single bus
- **Type Safety**: Events are defined as constants for type checking
- **Module Support**: Modules can define their own events and listen to app events
- **Cleanup Utilities**: Automatic cleanup to prevent memory leaks

## Using EventBus in the Main Application

### Importing the EventBus

```typescript
import { useEventBus, AppEvents } from '~/composables/useEventBus';

// Get a reference to the event bus
const eventBus = useEventBus();
```

### Emitting Events

```typescript
// Emit an event without payload
eventBus.emit(AppEvents.LOGOUT);

// Emit an event with payload
eventBus.emit(AppEvents.LOGIN, { user: userData });
```

### Subscribing to Events

```typescript
// Listen for an event
const unsubscribe = eventBus.on(AppEvents.LOGIN, (payload) => {
  console.log('User logged in:', payload.user);
});

// Remember to unsubscribe when component is unmounted
onBeforeUnmount(() => {
  unsubscribe();
});
```

### In Vue Components with Auto-Cleanup

```vue
<script setup>
import { useEventBus, AppEvents } from '~/composables/useEventBus';

const eventBus = useEventBus();
const messages = ref([]);

// Store unsubscribe functions
const cleanupFunctions = [];

// Listen for events
cleanupFunctions.push(
  eventBus.on(AppEvents.NEW_MESSAGE, (message) => {
    messages.value.push(message);
  })
);

// Clean up on component unmount
onBeforeUnmount(() => {
  cleanupFunctions.forEach(fn => fn());
});
</script>
```

## Using EventBus in Modules

### Accessing the EventBus in a Module

Modules can access the EventBus through the #imports utility:

```typescript
// In a module
import { defineNuxtPlugin } from '#app';
import { useEventBus, AppEvents } from '#imports';

export default defineNuxtPlugin(nuxtApp => {
  const eventBus = useEventBus();
  
  // Now you can use the event bus
});
```

### Alternative: Using Global Provider

If the module doesn't have direct access to EventBus through imports, you can use the global provider:

```typescript
// In a module
export default defineNuxtPlugin(nuxtApp => {
  const { $eventBus } = useNuxtApp();
  
  // Use $eventBus instead
});
```

### Creating an Event Handler Plugin for a Module

Create a dedicated plugin to handle module-specific event logic:

```typescript
// src/runtime/plugins/myModuleEvents.ts
import { defineNuxtPlugin } from '#app';
import { useEventBus, AppEvents } from '#imports';
import { MyModuleEvents } from '../constants/events';

export default defineNuxtPlugin(nuxtApp => {
  // Only run on client-side
  if (!process.client) return;
  
  const eventBus = useEventBus();
  const cleanupFunctions = [];
  
  // Listen for app events
  cleanupFunctions.push(
    eventBus.on(AppEvents.LOGIN, async () => {
      console.log('[my-module] User logged in, initializing module data');
      // Module-specific logic on login
    })
  );
  
  // Clean up on unmount
  nuxtApp.hook('app:beforeUnmount', () => {
    cleanupFunctions.forEach(fn => typeof fn === 'function' && fn());
  });
  
  return {
    // Optionally provide module-specific functions
    provide: {
      emitMyModuleEvent: (event, payload) => eventBus.emit(event, payload)
    }
  };
});
```

## Defining Custom Events

### Creating Module-Specific Events

Create a constants file in your module:

```typescript
// src/runtime/constants/events.ts
export const MyModuleEvents = {
  DATA_LOADED: 'my-module:data-loaded',
  ITEM_CREATED: 'my-module:item-created',
  ITEM_UPDATED: 'my-module:item-updated',
  ITEM_DELETED: 'my-module:item-deleted',
  ERROR_OCCURRED: 'my-module:error'
};
```

### Using Module Events

```typescript
import { useEventBus } from '#imports';
import { MyModuleEvents } from '../constants/events';

export const useMyModuleStore = defineStore('myModule', {
  // ... state and getters
  
  actions: {
    async createItem(item) {
      const eventBus = useEventBus();
      
      try {
        // Create the item
        const result = await $api.post('/items', item);
        
        // Emit success event
        eventBus.emit(MyModuleEvents.ITEM_CREATED, result);
        
        return result;
      } catch (error) {
        // Emit error event
        eventBus.emit(MyModuleEvents.ERROR_OCCURRED, { 
          action: 'create', 
          error 
        });
        throw error;
      }
    }
  }
});
```

## Best Practices

### 1. Use Namespaced Events

Prefix your event names with your module name to avoid conflicts:

```typescript
// Good
const ModuleEvents = {
  ITEM_UPDATED: 'module-name:item-updated'
};

// Avoid
const ModuleEvents = {
  ITEM_UPDATED: 'item-updated' // Could conflict with other modules
};
```

### 2. Clean Up Event Listeners

Always unsubscribe from events when components or plugins are unmounted:

```typescript
// In a component
const unsubscribe = eventBus.on(AppEvents.LOGIN, handleLogin);
onBeforeUnmount(() => unsubscribe());

// In a plugin
const cleanupFunctions = [];
cleanupFunctions.push(eventBus.on(AppEvents.LOGIN, handleLogin));
nuxtApp.hook('app:beforeUnmount', () => {
  cleanupFunctions.forEach(fn => fn());
});
```

### 3. Keep Payloads Simple and Serializable

Event payloads should be simple data objects without methods or complex references:

```typescript
// Good
eventBus.emit(ModuleEvents.ITEM_UPDATED, { 
  id: item.id, 
  name: item.name,
  updatedAt: new Date().toISOString()
});

// Avoid
eventBus.emit(ModuleEvents.ITEM_UPDATED, item); // item might have methods or circular references
```

### 4. Document Events in a Central Location

Keep a master list of all events and their expected payloads:

```typescript
/**
 * @event auth:login
 * @description Emitted when a user successfully logs in
 * @payload {object} payload
 * @payload {object} payload.user - The user object
 */

/**
 * @event settings:saved
 * @description Emitted when settings are successfully saved
 * @payload {object} payload
 * @payload {string} payload.group - The settings group
 * @payload {string} payload.subgroup - The settings subgroup (optional)
 * @payload {array} payload.settings - The saved settings
 */
```

## Troubleshooting

### Events Not Being Received

1. **Check Import Paths**: Ensure you're importing useEventBus from the correct location
2. **Verify Event Names**: Make sure the event name strings match exactly
3. **Client vs. Server**: Remember that events only work on the client side
4. **Check Cleanup Logic**: Ensure unsubscribe functions aren't being called too early

### Memory Leaks

If your application has memory leaks, check for:

1. **Missing Unsubscribe Calls**: Ensure all event listeners are being unsubscribed
2. **Component Lifecycle**: Make sure unsubscribe functions are called in onBeforeUnmount
3. **Duplicate Listeners**: Check you're not adding the same listener multiple times

---

For questions or updates to this documentation, please contact the application architecture team.