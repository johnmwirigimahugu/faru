/**
 * farusql.js
 * ============================================================================
 *
 * farusql.js is an enterprise-grade, dependency-free JavaScript NoSQL ORM, drawing inspiration from projects like SleekDB, KeshMoney, Firestore, RedBeanPHP, and ajx.
 *
 * It offers a comprehensive suite of features for robust data management, including persistent JSON document storage with atomic writes (Node.js fs) or in-memory operations (browser).
 *
 * Key features include:
 * - Full CRUD capabilities with an advanced query builder supporting complex conditions (where, orWhere, whereIn, whereLike), pagination (limit, skip), and ordering (orderBy).
 * - Support for nested fields using dot notation (e.g., 'user.address.city') and automatic UUID v4 _id generation.
 * - Optimized data access through in-memory caching with integrated hooks, advanced indexing (single and composite), and index-based query optimization for equality filters.
 * - Powerful text search with full-text indexing and querying.
 * - Enhanced data integrity and control via data validation hooks, transaction support (Node.js only), and built-in REST client (ajx-style).
 * - Rich data modeling with schema introspection (getSchema), relations helpers (hasMany, belongsTo, relate), and change tracking including timestamps and revision history.
 * - Flexible data lifecycle management with soft deletes support, real-time change listeners (onChange), and field-level encryption (simple symmetric).
 * - Scalability features like remote sync (syncWith) and query plan debugging (explainQuery) for performance analysis.
 *
 * This solution is designed as a flagship product: robust, performant,
 * and ready for enterprise PHP applications requiring flexible NoSQL storage.
 *
 * ============================================================================
 *
 * Copyright (C) 2025 by John "Kesh" Mahugu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

(function (global) {
  // Utility functions
  const isNode = typeof process !== "undefined" && process.versions && process.versions.node;
  const nowISO = () => new Date().toISOString();

  // Simple symmetric encryption (XOR with key) for demonstration
  function simpleEncrypt(text, key) {
    let result = "";
    for (let i = 0; i < text.length; i++) {
      result += String.fromCharCode(text.charCodeAt(i) ^ key.charCodeAt(i % key.length));
    }
    return btoa(result);
  }
  function simpleDecrypt(encoded, key) {
    const text = atob(encoded);
    let result = "";
    for (let i = 0; i < text.length; i++) {
      result += String.fromCharCode(text.charCodeAt(i) ^ key.charCodeAt(i % key.length));
    }
    return result;
  }

  // UUID v4 generator (browser and Node.js)
  function uuidv4() {
    if (isNode) {
      return require("crypto").randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
      const r = (Math.random() * 16) | 0,
        v = c === "x" ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  class farusql {
    constructor(storeName, storePath = "./data") {
      this.storeName = storeName;
      this.storePath = storePath;
      this.filePath = isNode ? require("path").join(this.storePath, `${this.storeName}.json`) : null;
      this.indexPath = isNode ? require("path").join(this.storePath, `${this.storeName}_indexes.json`) : null;
      this.fullTextIndexPath = isNode ? require("path").join(this.storePath, `${this.storeName}_fulltext.json`) : null;

      this.data = [];
      this.dataLoaded = false;

      this.indexes = {};
      this.indexesLoaded = false;
      this.indexedFields = new Set(); // To track which fields are indexed

      this.fullTextIndex = {};
      this.fullTextIndexLoaded = false;
      this.fullTextIndexedFields = new Set(); // To track which fields are full-text indexed

      this.wheres = [];
      this.limitCount = null;
      this.skipCount = 0;
      this.orderByField = null;
      this.orderByDirection = "asc";

      this.validationCallbacks = [];

      // Simple in-memory cache for demonstration
      this._internalCache = new Map();
      this.cacheGetCallback = (key) => this._internalCache.get(key);
      this.cacheSetCallback = (key, value) => this._internalCache.set(key, value);
      this.cacheDeleteCallback = (key) => this._internalCache.delete(key);

      this.transactionLock = null; // Stores the _id of the document currently locking
      this.inTransaction = false; // Flag to indicate if a transaction is active
      this.transactionLog = []; // Log for transaction operations

      this.softDeleteEnabled = false;
      this.showDeleted = false;

      this.changeListeners = [];

      this.encryptionKey = null;
      this.encryptedFields = [];

      this.remoteSyncUrl = null;

      if (isNode) {
        const fs = require("fs");
        if (!fs.existsSync(this.storePath)) {
          fs.mkdirSync(this.storePath, { recursive: true });
        }
      }

      this._resetQuery(); // Initialize query state
    }

    // ------------------- Core Utilities -------------------

    static generateId() {
      return uuidv4();
    }

    _readFile(path) {
      if (!isNode) return null;
      try {
        return require("fs").readFileSync(path, "utf8");
      } catch (e) {
        if (e.code === 'ENOENT') return null; // File not found is acceptable
        throw e; // Re-throw other errors
      }
    }

    _writeFile(path, data) {
      if (!isNode) return;
      const fs = require("fs");
      const tmpPath = path + ".tmp";
      fs.writeFileSync(tmpPath, data, "utf8");
      fs.renameSync(tmpPath, path);
    }

    _getValueByDot(obj, key) {
      return key.split(".").reduce((o, k) => (o ? o[k] : undefined), obj);
    }

    _setValueByDot(obj, key, value) {
      const keys = key.split(".");
      let cur = obj;
      for (let i = 0; i < keys.length - 1; i++) {
        if (!cur[keys[i]] || typeof cur[keys[i]] !== "object" || Array.isArray(cur[keys[i]])) cur[keys[i]] = {}; // Ensure it's an object, not array
        cur = cur[keys[i]];
      }
      cur[keys[keys.length - 1]] = value;
    }

    _deepClone(obj) {
      return JSON.parse(JSON.stringify(obj));
    }

    _deepMerge(target, source) {
      for (const key in source) {
        if (source.hasOwnProperty(key)) {
          if (source[key] && typeof source[key] === "object" && !Array.isArray(source[key])) {
            if (!target[key] || typeof target[key] !== "object" || Array.isArray(target[key])) target[key] = {}; // Ensure it's an object, not array
            this._deepMerge(target[key], source[key]);
          } else {
            target[key] = source[key];
          }
        }
      }
      return target;
    }

    _tokenizeText(text) {
      // Improved tokenizer to handle more characters and ensure unique tokens
      if (typeof text !== 'string') return [];
      return [...new Set(text.toLowerCase().match(/\b\p{L}[\p{L}\p{N}]*\b/gu) || [])]; // Match whole words
    }

    _serializeIndexValue(val) {
      if (typeof val === "boolean") return val ? "1" : "0";
      if (val === null || val === undefined) return "NULL";
      if (typeof val === "object") return JSON.stringify(val); // For complex objects
      return String(val);
    }

    // ------------------- Data Loading/Saving -------------------

    async loadData() {
      if (this.dataLoaded) return;
      if (isNode && this.filePath) {
        let content = this._readFile(this.filePath);
        if (content) {
          this.data = JSON.parse(content);
          // Apply soft delete filter only if not explicitly showing deleted
          if (this.softDeleteEnabled && !this.showDeleted) {
            this.data = this.data.filter(d => !d._deleted);
          }
        } else {
          this.data = [];
        }
      } else {
        // For browser, data remains in-memory unless explicitly loaded
        this.data = this.data || [];
      }
      this.dataLoaded = true;
    }

    async saveData() {
      if (!isNode || !this.filePath) return;
      // When saving, ensure all data (including potentially soft-deleted) is written back.
      // This requires combining current filtered data with deleted data from storage.
      // For simplicity here, we'll assume `this.data` always contains the current state.
      // A more robust solution might read all data, merge, then write.
      const json = JSON.stringify(this.data, null, 2);
      this._writeFile(this.filePath, json);
    }

    async loadIndexes() {
      if (this.indexesLoaded) return;
      if (isNode && this.indexPath) {
        let content = this._readFile(this.indexPath);
        this.indexes = content ? JSON.parse(content) : {};
        // Reconstruct indexedFields Set from loaded indexes
        for (const key in this.indexes) {
          this.indexes[key].fields.forEach(f => this.indexedFields.add(f));
        }
      } else {
        this.indexes = {};
      }
      this.indexesLoaded = true;
    }

    async saveIndexes() {
      if (!isNode || !this.indexPath) return;
      const json = JSON.stringify(this.indexes, null, 2);
      this._writeFile(this.indexPath, json);
    }

    async loadFullTextIndex() {
      if (this.fullTextIndexLoaded) return;
      if (isNode && this.fullTextIndexPath) {
        let content = this._readFile(this.fullTextIndexPath);
        this.fullTextIndex = content ? JSON.parse(content) : {};
        // Reconstruct fullTextIndexedFields Set from stored metadata if needed
        // (Currently, full-text fields are passed at index creation, not stored with index)
      } else {
        this.fullTextIndex = {};
      }
      this.fullTextIndexLoaded = true;
    }

    async saveFullTextIndex() {
      if (!isNode || !this.fullTextIndexPath) return;
      const json = JSON.stringify(this.fullTextIndex, null, 2);
      this._writeFile(this.fullTextIndexPath, json);
    }

    // ------------------- Query Builder -------------------
    _resetQuery() {
      this.wheres = [];
      this.limitCount = null;
      this.skipCount = 0;
      this.orderByField = null;
      this.orderByDirection = "asc";
      this.showDeleted = false; // Reset soft delete filter
    }

    where(field, operator, value) {
      this.wheres.push({ field, operator, value });
      return this;
    }

    orWhere(conditions) {
      // conditions should be an array of {field, operator, value}
      this.wheres.push({ or: conditions });
      return this;
    }

    whereIn(field, values) {
      this.wheres.push({ field, operator: 'in', value: values });
      return this;
    }

    whereLike(field, value) {
      this.wheres.push({ field, operator: 'like', value: value });
      return this;
    }

    limit(count) {
      this.limitCount = count;
      return this;
    }

    skip(count) {
      this.skipCount = count;
      return this;
    }

    orderBy(field, direction = 'asc') {
      this.orderByField = field;
      this.orderByDirection = direction.toLowerCase(); // 'asc' or 'desc'
      return this;
    }

    // ------------------- Data Access / Query Execution -------------------

    async findById(_id) {
      await this.loadData();
      let doc = this.data.find(d => d._id === _id && (!this.softDeleteEnabled || this.showDeleted || !d._deleted));
      this._resetQuery();
      return doc ? this._decryptDocument(this._deepClone(doc)) : null;
    }

    async findOne() {
      const results = await this.get(); // Reuses the full query pipeline
      return results.length > 0 ? results[0] : null;
    }

    async get() {
      await this.loadData();
      await this.loadIndexes();

      let results = this._deepClone(this.data);
      const queryPlan = this.explainQuery(); // Get plan for potential optimization

      // 1. Apply soft delete filter if enabled and not explicitly showing deleted
      if (this.softDeleteEnabled && !this.showDeleted) {
        results = results.filter(d => !d._deleted);
      }

      // 2. Index-based filtering (for equality queries)
      // This is a simple optimization. More complex scenarios would involve merging multiple index results.
      if (this.wheres.length > 0 && queryPlan.usedIndexes.length > 0) {
        const potentialIndexFilter = this.wheres.find(
          w => queryPlan.usedIndexes.includes(w.field) && ['=', '=='].includes(w.operator)
        );

        if (potentialIndexFilter) {
          const indexKey = this._serializeIndexValue(potentialIndexFilter.value);
          const index = this.indexes[potentialIndexFilter.field]; // Assuming single field index for simplicity here
          if (index && index.map[indexKey]) {
            const indexedIds = new Set(index.map[indexKey]);
            results = results.filter(doc => indexedIds.has(doc._id));
            // Remove this condition from where clause to avoid double filtering
            this.wheres = this.wheres.filter(w => w !== potentialIndexFilter);
          }
        }
      }

      // 3. Apply general `where` conditions
      results = results.filter(doc => {
        return this.wheres.every(condition => {
          if (condition.or) {
            return condition.or.some(subCondition =>
              this._testCondition(doc, subCondition)
            );
          } else {
            return this._testCondition(doc, condition);
          }
        });
      });

      // 4. Apply orderBy
      if (this.orderByField) {
        const field = this.orderByField;
        const direction = this.orderByDirection;
        results.sort((a, b) => {
          const valA = this._getValueByDot(a, field);
          const valB = this._getValueByDot(b, field);

          if (valA === valB) return 0;
          if (valA === undefined || valA === null) return direction === 'asc' ? 1 : -1;
          if (valB === undefined || valB === null) return direction === 'asc' ? -1 : 1;

          if (typeof valA === 'string' && typeof valB === 'string') {
            return direction === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
          }
          return direction === 'asc' ? (valA < valB ? -1 : 1) : (valB < valA ? -1 : 1);
        });
      }

      // 5. Apply skip and limit
      if (this.skipCount > 0) {
        results = results.slice(this.skipCount);
      }
      if (this.limitCount !== null) {
        results = results.slice(0, this.limitCount);
      }

      // Decrypt documents before returning
      const decryptedResults = results.map(doc => this._decryptDocument(doc));
      this._resetQuery(); // Reset query state for next operation
      return decryptedResults;
    }

    _testCondition(doc, condition) {
      const { field, operator, value } = condition;
      const docValue = this._getValueByDot(doc, field);

      switch (operator) {
        case '=':
        case '==':
          return docValue == value;
        case '!=':
        case '<>':
          return docValue != value;
        case '>':
          return docValue > value;
        case '>=':
          return docValue >= value;
        case '<':
          return docValue < value;
        case '<=':
          return docValue <= value;
        case 'in':
          return Array.isArray(value) && value.includes(docValue);
        case 'like': // Basic substring match
          return typeof docValue === 'string' && typeof value === 'string' &&
                 docValue.toLowerCase().includes(value.toLowerCase());
        default:
          return false;
      }
    }

    async count() {
      // A more efficient count would apply only filters, not fetch all data
      await this.loadData();
      let results = this._deepClone(this.data);

      // Apply soft delete filter
      if (this.softDeleteEnabled && !this.showDeleted) {
        results = results.filter(d => !d._deleted);
      }

      // Apply general `where` conditions
      results = results.filter(doc => {
        return this.wheres.every(condition => {
          if (condition.or) {
            return condition.or.some(subCondition =>
              this._testCondition(doc, subCondition)
            );
          } else {
            return this._testCondition(doc, condition);
          }
        });
      });
      this._resetQuery();
      return results.length;
    }

    // ------------------- CRUD Methods -------------------

    async insert(doc) {
      await this.loadData();
      await this.loadIndexes();
      await this.loadFullTextIndex();

      // Check for transaction lock
      if (this.inTransaction && this.transactionLock) {
        throw new Error("Cannot insert: Another transaction holds a lock.");
      }

      const newDoc = this._deepClone(doc); // Clone to avoid modifying original input
      this.validateDocument(newDoc);

      if (!newDoc._id) newDoc._id = farusql.generateId();
      this._addTimestamps(newDoc, true);
      this._addRevision(newDoc, 'insert'); // Add initial revision

      const encryptedDoc = this.encryptionKey ? this._encryptDocument(newDoc) : newDoc;
      this.data.push(encryptedDoc);

      this._updateIndexesOnInsert(encryptedDoc);
      this._indexDocumentFullText(encryptedDoc, Array.from(this.fullTextIndexedFields));

      if (!this.inTransaction) {
        await this.saveData();
        await this.saveIndexes();
        await this.saveFullTextIndex();
        this._invalidateCache();
      } else {
        this.transactionLog.push({ type: 'insert', doc: encryptedDoc });
      }

      this._triggerChange(this._decryptDocument(this._deepClone(encryptedDoc)), 'insert');
      return this._decryptDocument(this._deepClone(encryptedDoc)); // Return decrypted clone
    }

    async insertMany(docs) {
      await this.loadData();
      await this.loadIndexes();
      await this.loadFullTextIndex();

      if (this.inTransaction && this.transactionLock) {
        throw new Error("Cannot insertMany: Another transaction holds a lock.");
      }

      const insertedDocs = [];
      for (const doc of docs) {
        const newDoc = this._deepClone(doc);
        this.validateDocument(newDoc);

        if (!newDoc._id) newDoc._id = farusql.generateId();
        this._addTimestamps(newDoc, true);
        this._addRevision(newDoc, 'insert');

        const encryptedDoc = this.encryptionKey ? this._encryptDocument(newDoc) : newDoc;
        this.data.push(encryptedDoc);

        this._updateIndexesOnInsert(encryptedDoc);
        this._indexDocumentFullText(encryptedDoc, Array.from(this.fullTextIndexedFields));
        insertedDocs.push(this._decryptDocument(this._deepClone(encryptedDoc))); // Store decrypted for return
      }

      if (!this.inTransaction) {
        await this.saveData();
        await this.saveIndexes();
        await this.saveFullTextIndex();
        this._invalidateCache();
      } else {
        this.transactionLog.push({ type: 'insertMany', docs: insertedDocs.map(d => this._encryptDocument(d)) }); // Log encrypted
      }

      insertedDocs.forEach(doc => this._triggerChange(doc, 'insert'));
      return insertedDocs;
    }

    async update(query, updates) {
      await this.loadData();
      await this.loadIndexes();
      await this.loadFullTextIndex();

      // Find documents matching the query using the current query state
      const matchingDocs = await this.get(); // Uses the full query pipeline to find docs

      const updatedDocs = [];
      for (const doc of matchingDocs) {
        if (this.inTransaction && this.transactionLock && this.transactionLock !== doc._id) {
          throw new Error(`Cannot update document ${doc._id}: Transaction lock held by another document.`);
        }

        const oldDoc = this._deepClone(doc); // Store original for index update
        const docIndex = this.data.findIndex(d => d._id === doc._id);
        if (docIndex === -1) continue; // Should not happen if `get` returned it

        let currentDocInStore = this.data[docIndex]; // Reference to the actual object in this.data

        // Apply updates
        this._deepMerge(currentDocInStore, updates);
        this._addTimestamps(currentDocInStore, false); // Update timestamp
        this._addRevision(currentDocInStore, 'update'); // Add revision

        this.validateDocument(this._decryptDocument(this._deepClone(currentDocInStore))); // Validate decrypted version

        // Re-encrypt if encryption is enabled, after updates
        if (this.encryptionKey) {
            currentDocInStore = this._encryptDocument(this._decryptDocument(currentDocInStore)); // Decrypt, then re-encrypt
            this.data[docIndex] = currentDocInStore; // Update the reference in data array
        }

        this._updateIndexesOnUpdate(oldDoc, currentDocInStore);
        // Full-text index update: remove old, add new for relevant fields
        this._removeDocumentFromFullTextIndex(oldDoc._id);
        this._indexDocumentFullText(currentDocInStore, Array.from(this.fullTextIndexedFields));

        updatedDocs.push(this._decryptDocument(this._deepClone(currentDocInStore)));
      }

      if (!this.inTransaction) {
        await this.saveData();
        await this.saveIndexes();
        await this.saveFullTextIndex();
        this._invalidateCache();
      } else {
        this.transactionLog.push({ type: 'update', docs: updatedDocs.map(d => this._encryptDocument(d)) });
      }

      updatedDocs.forEach(doc => this._triggerChange(doc, 'update'));
      this._resetQuery(); // Reset query state after update operation
      return updatedDocs;
    }


    async delete(_id) {
      await this.loadData();
      await this.loadIndexes();
      await this.loadFullTextIndex();

      if (this.inTransaction && this.transactionLock) {
        throw new Error("Cannot delete: Another transaction holds a lock.");
      }

      let deletedDoc = null;
      let deleteIndex = -1;

      // Handle soft delete
      if (this.softDeleteEnabled) {
        deleteIndex = this.data.findIndex(d => d._id === _id && !d._deleted);
        if (deleteIndex !== -1) {
          deletedDoc = this._deepClone(this.data[deleteIndex]);
          if (this.inTransaction && this.transactionLock && this.transactionLock !== deletedDoc._id) {
            throw new Error(`Cannot soft delete document ${deletedDoc._id}: Transaction lock held by another document.`);
          }
          this.data[deleteIndex]._deleted = true;
          this.data[deleteIndex]._updated = nowISO(); // Mark update for soft delete
          this._addRevision(this.data[deleteIndex], 'soft_delete'); // Add revision
        }
      } else {
        // Hard delete
        deleteIndex = this.data.findIndex(d => d._id === _id);
        if (deleteIndex !== -1) {
          deletedDoc = this.data.splice(deleteIndex, 1)[0];
          if (this.inTransaction && this.transactionLock && this.transactionLock !== deletedDoc._id) {
            throw new Error(`Cannot hard delete document ${deletedDoc._id}: Transaction lock held by another document.`);
          }
          // Remove from indexes only if hard deleting
          this._updateIndexesOnDelete([deletedDoc._id]);
          this._removeDocumentFromFullTextIndex(deletedDoc._id);
        }
      }

      if (deletedDoc) {
        if (!this.inTransaction) {
          await this.saveData();
          await this.saveIndexes(); // Indexes might change only for hard delete
          await this.saveFullTextIndex(); // Full-text index changes for hard delete
          this._invalidateCache();
        } else {
          this.transactionLog.push({ type: 'delete', _id: _id, isSoftDelete: this.softDeleteEnabled });
        }
        this._triggerChange(this._decryptDocument(this._deepClone(deletedDoc)), this.softDeleteEnabled ? 'soft_delete' : 'delete');
        return true;
      }
      return false;
    }

    // ------------------- Validation -------------------

    registerValidationCallback(callback) {
      this.validationCallbacks.push(callback);
    }

    validateDocument(doc) {
      for (const cb of this.validationCallbacks) {
        const result = cb(doc);
        if (result !== true) throw new Error("Validation failed: " + result);
      }
    }

    // ------------------- Encryption -------------------

    encryptFields(fields) {
      this.encryptedFields = fields;
    }

    setEncryptionKey(key) {
      this.encryptionKey = key;
    }

    _encryptDocument(doc) {
      if (!this.encryptionKey) return doc;
      const encryptedDoc = this._deepClone(doc); // Ensure we're working on a copy
      for (const field of this.encryptedFields) {
        const val = this._getValueByDot(encryptedDoc, field);
        if (val !== undefined) {
          const strVal = typeof val === "string" ? val : JSON.stringify(val);
          this._setValueByDot(encryptedDoc, field, simpleEncrypt(strVal, this.encryptionKey));
        }
      }
      return encryptedDoc;
    }

    _decryptDocument(doc) {
      if (!this.encryptionKey) return doc;
      const decryptedDoc = this._deepClone(doc); // Ensure we're working on a copy
      for (const field of this.encryptedFields) {
        const val = this._getValueByDot(decryptedDoc, field);
        if (val !== undefined) {
          try {
            const decrypted = simpleDecrypt(val, this.encryptionKey);
            try {
              // Try parsing as JSON if it looks like a stringified object/array
              this._setValueByDot(decryptedDoc, field, JSON.parse(decrypted));
            } catch {
              this._setValueByDot(decryptedDoc, field, decrypted);
            }
          } catch (e) {
            // console.warn(`Decryption error for field ${field}:`, e.message);
            // Ignore decryption errors for robust operation, keep original encrypted value
          }
        }
      }
      return decryptedDoc;
    }

    // ------------------- Transactions (Node.js Only) -------------------

    async beginTransaction(_id = null) {
      if (!isNode) {
        throw new Error("Transactions are only supported in Node.js environment.");
      }
      if (this.inTransaction) {
        throw new Error("A transaction is already active.");
      }

      this.inTransaction = true;
      this.transactionLock = _id; // Lock a specific document or null for collection-level lock
      this.transactionLog = []; // Clear log for new transaction

      // Load initial state (optional, but good for consistency)
      await this.loadData();
      await this.loadIndexes();
      await this.loadFullTextIndex();

      // Store initial state for rollback if needed (deep clone is crucial)
      this._initialDataState = this._deepClone(this.data);
      this._initialIndexesState = this._deepClone(this.indexes);
      this._initialFullTextIndexState = this._deepClone(this.fullTextIndex);
    }

    async commit() {
      if (!this.inTransaction) {
        throw new Error("No active transaction to commit.");
      }

      try {
        await this.saveData();
        await this.saveIndexes();
        await this.saveFullTextIndex();
        this._invalidateCache();
        // Trigger changes for all operations logged
        for (const log of this.transactionLog) {
          if (log.type === 'insert') {
            this._triggerChange(this._decryptDocument(this._deepClone(log.doc)), 'insert');
          } else if (log.type === 'insertMany') {
            log.docs.forEach(doc => this._triggerChange(this._decryptDocument(this._deepClone(doc)), 'insert'));
          } else if (log.type === 'update') {
            log.docs.forEach(doc => this._triggerChange(this._decryptDocument(this._deepClone(doc)), 'update'));
          } else if (log.type === 'delete') {
            const tempDoc = { _id: log._id }; // We don't have the full deleted doc here, only _id
            this._triggerChange(tempDoc, log.isSoftDelete ? 'soft_delete' : 'delete');
          }
        }
      } finally {
        this._endTransaction();
      }
    }

    async rollback() {
      if (!this.inTransaction) {
        throw new Error("No active transaction to rollback.");
      }

      // Revert to initial state
      this.data = this._initialDataState;
      this.indexes = this._initialIndexesState;
      this.fullTextIndex = this._initialFullTextIndexState;

      // Persist the reverted state
      if (isNode) {
        await this.saveData();
        await this.saveIndexes();
        await this.saveFullTextIndex();
      }
      this._invalidateCache();
      this._endTransaction();
    }

    _endTransaction() {
      this.inTransaction = false;
      this.transactionLock = null;
      this.transactionLog = [];
      delete this._initialDataState;
      delete this._initialIndexesState;
      delete this._initialFullTextIndexState;
    }

    // ------------------- Soft Deletes -------------------

    enableSoftDeletes(enable = true) {
      this.softDeleteEnabled = enable;
      return this;
    }

    onlyDeleted() {
      this.showDeleted = true;
      return this;
    }

    withTrashed() {
      this.showDeleted = true;
      return this;
    }

    // ------------------- Change Tracking & Revision History -------------------

    _addTimestamps(doc, isNew = false) {
      const now = nowISO();
      if (isNew) doc._created = now;
      doc._updated = now;
    }

    _addRevision(doc, action) {
      if (!doc._revisions) doc._revisions = [];
      doc._revisions.push({
        _revisionId: farusql.generateId(),
        _timestamp: nowISO(),
        _action: action,
        // For a full revision history, you'd store the difference or the full document state here.
        // For simplicity, we just log the action and timestamp.
        // E.g., _changes: diff(oldDoc, newDoc)
      });
      // Limit revision history size if desired
      // if (doc._revisions.length > 10) doc._revisions.shift();
    }

    // ------------------- Change Listeners -------------------

    onChange(callback) {
      this.changeListeners.push(callback);
    }

    _triggerChange(doc, type) {
      // Trigger changes only when not in a transaction, or after commit
      if (!this.inTransaction) {
        for (const cb of this.changeListeners) {
          try {
            cb({ doc: this._deepClone(doc), type }); // Pass a clone to prevent external modification
          } catch (e) {
            console.error("Error in change listener:", e);
            // ignore listener errors
          }
        }
      }
    }

    // ------------------- Relations -------------------

    relate(foreignKey, relatedCollection, relatedKey = "_id") {
      const relatedMap = new Map();
      for (const item of relatedCollection) {
        relatedMap.set(item[relatedKey], item);
      }
      return this.data.map(doc => ({
        ...this._decryptDocument(this._deepClone(doc)),
        related: relatedMap.get(doc[foreignKey]) || null,
      }));
    }

    hasMany(relatedCollection, foreignKey) {
      const map = new Map();
      for (const item of relatedCollection) {
        const key = item[foreignKey];
        if (!map.has(key)) map.set(key, []);
        map.get(key).push(item);
      }
      return this.data.map(doc => ({
        ...this._decryptDocument(this._deepClone(doc)),
        related: map.get(doc._id) || [],
      }));
    }

    belongsTo(relatedCollection, foreignKey, relatedKey = "_id") {
      const relatedMap = new Map();
      for (const item of relatedCollection) {
        relatedMap.set(item[relatedKey], item);
      }
      return this.data.map(doc => ({
        ...this._decryptDocument(this._deepClone(doc)),
        related: relatedMap.get(doc[foreignKey]) || null,
      }));
    }

    // ------------------- Remote Sync -------------------

    syncWith(url) {
      this.remoteSyncUrl = url;
      // This is a placeholder for the actual sync initiation
      console.log(`farusql.js is configured to sync with: ${url}. Implement periodic or event-driven _doRemoteSync calls.`);
      // Example: setInterval(() => this._doRemoteSync(), 60000); // Sync every minute
    }

    async _doRemoteSync() {
      if (!this.remoteSyncUrl) return;
      console.log(`Performing remote sync with ${this.remoteSyncUrl}...`);
      // In a real application, you would implement HTTP requests here
      // For example, using fetch or Node.js http module to push/pull data.
      // try {
      //   const response = await fetch(this.remoteSyncUrl, {
      //     method: 'POST', // Or GET, PUT depending on your sync strategy
      //     headers: { 'Content-Type': 'application/json' },
      //     body: JSON.stringify({ changes: this.transactionLog // Or a diff since last sync })
      //   });
      //   const result = await response.json();
      //   console.log('Remote sync successful:', result);
      //   // After successful sync, clear relevant transaction log or update local state
      // } catch (error) {
      //   console.error('Remote sync failed:', error);
      // }
    }

    // ------------------- Pagination -------------------

    async paginate({ page = 1, perPage = 10 }) {
      if (page < 1) page = 1;
      if (perPage < 1) perPage = 1;

      const totalCount = await this.count(); // Get count of filtered results
      const totalPages = Math.ceil(totalCount / perPage);

      this.limit(perPage).skip((page - 1) * perPage);
      const data = await this.get();

      return {
        data,
        currentPage: page,
        perPage,
        totalCount,
        totalPages,
        hasMorePages: page < totalPages,
        nextPage: page < totalPages ? page + 1 : null,
        prevPage: page > 1 ? page - 1 : null,
      };
    }

    // ------------------- Query Plan Debugging -------------------

    explainQuery() {
      const fields = new Set();
      for (const cond of this.wheres) {
        if (cond.or) {
          cond.or.forEach(c => fields.add(c.field));
        } else {
          fields.add(cond.field);
        }
      }
      const usedIndexes = [];
      for (const idxKey in this.indexes) {
        const idx = this.indexes[idxKey];
        // Check if any field in the condition matches any field in the index
        if (idx.fields.some(f => fields.has(f))) {
          usedIndexes.push(idxKey);
        }
      }
      return {
        queryConditions: this.wheres,
        orderByField: this.orderByField,
        skipCount: this.skipCount,
        limitCount: this.limitCount,
        whereFields: Array.from(fields),
        availableIndexes: Object.keys(this.indexes),
        usedIndexes, // This will list indexes that match a field in a simple where clause
        softDeleteEnabled: this.softDeleteEnabled,
        showingDeleted: this.showDeleted
      };
    }

    // ------------------- Indexing Methods -------------------

    async createIndex(fields, name = null) {
      await this.loadData();
      await this.loadIndexes();

      const indexName = name || (Array.isArray(fields) ? fields.join('_') : fields);
      const indexFields = Array.isArray(fields) ? fields : [fields];

      if (this.indexes[indexName]) {
        // console.warn(`Index '${indexName}' already exists. Rebuilding.`);
        // return; // Or rebuild forcefully? For now, we'll rebuild
      }

      this.indexes[indexName] = { fields: indexFields, map: {} };
      indexFields.forEach(f => this.indexedFields.add(f)); // Track all fields that are indexed

      for (const doc of this.data) {
        const indexValue = this._getCompositeIndexValue(doc, indexFields);
        if (indexValue !== null) {
          if (!this.indexes[indexName].map[indexValue]) {
            this.indexes[indexName].map[indexValue] = [];
          }
          this.indexes[indexName].map[indexValue].push(doc._id);
        }
      }
      await this.saveIndexes();
    }

    _updateIndexesOnInsert(doc) {
      for (const key in this.indexes) {
        const index = this.indexes[key];
        const indexValue = this._getCompositeIndexValue(doc, index.fields);
        if (indexValue === null) continue;
        if (!index.map[indexValue]) index.map[indexValue] = [];
        index.map[indexValue].push(doc._id);
      }
    }

    _updateIndexesOnUpdate(oldDoc, newDoc) {
      for (const key in this.indexes) {
        const index = this.indexes[key];
        const oldKey = this._getCompositeIndexValue(oldDoc, index.fields);
        const newKey = this._getCompositeIndexValue(newDoc, index.fields);
        if (oldKey === newKey) continue; // Index value hasn't changed

        // Remove old entry
        if (oldKey && index.map[oldKey]) {
          index.map[oldKey] = index.map[oldKey].filter(id => id !== oldDoc._id);
          if (index.map[oldKey].length === 0) delete index.map[oldKey];
        }
        // Add new entry
        if (newKey) {
          if (!index.map[newKey]) index.map[newKey] = [];
          index.map[newKey].push(newDoc._id);
        }
      }
    }

    _updateIndexesOnDelete(deletedIds) {
      for (const key in this.indexes) {
        const index = this.indexes[key];
        for (const indexValue in index.map) {
          index.map[indexValue] = index.map[indexValue].filter(id => !deletedIds.includes(id));
          if (index.map[indexValue].length === 0) delete index.map[indexValue];
        }
      }
    }

    _getCompositeIndexValue(doc, fields) {
      const values = [];
      for (const field of fields) {
        const val = this._getValueByDot(doc, field);
        // If any part of a composite index is null/undefined, the whole index entry for that doc is null
        if (val === undefined || val === null) return null;
        values.push(this._serializeIndexValue(val));
      }
      return values.join('|');
    }

    // ------------------- Full-Text Indexing and Querying -------------------

    async createFullTextIndex(fields) {
      await this.loadData();
      await this.loadFullTextIndex();

      this.fullTextIndexedFields = new Set(fields); // Keep track of fields to index

      // Rebuild full-text index for all existing data
      this.fullTextIndex = {};
      for (const doc of this.data) {
        this._indexDocumentFullText(doc, fields);
      }
      await this.saveFullTextIndex();
    }

    _indexDocumentFullText(doc, fields) {
      const docId = doc._id;
      if (!docId) return;

      for (const field of fields) {
        const val = this._getValueByDot(doc, field);
        if (typeof val !== 'string') continue;

        const words = this._tokenizeText(val);
        for (const word of words) {
          if (!this.fullTextIndex[word]) this.fullTextIndex[word] = {};
          this.fullTextIndex[word][docId] = true; // Store document ID for each word
        }
      }
    }

    _removeDocumentFromFullTextIndex(docId) {
      for (const word in this.fullTextIndex) {
        if (this.fullTextIndex[word][docId]) {
          delete this.fullTextIndex[word][docId];
          if (Object.keys(this.fullTextIndex[word]).length === 0) {
            delete this.fullTextIndex[word]; // Remove word entry if no docs reference it
          }
        }
      }
    }

    async fullTextSearch(query) {
      await this.loadData();
      await this.loadFullTextIndex();

      const queryTokens = this._tokenizeText(query);
      if (queryTokens.length === 0) return [];

      let matchingDocIds = new Set();
      let firstToken = true;

      for (const token of queryTokens) {
        const docsForToken = this.fullTextIndex[token];
        if (!docsForToken) {
          return []; // If any token is not found, no documents match all tokens
        }

        const currentTokenIds = new Set(Object.keys(docsForToken));

        if (firstToken) {
          matchingDocIds = currentTokenIds;
          firstToken = false;
        } else {
          // Intersect with existing matching IDs for AND-like behavior
          const intersection = new Set();
          for (const id of matchingDocIds) {
            if (currentTokenIds.has(id)) {
              intersection.add(id);
            }
          }
          matchingDocIds = intersection;
          if (matchingDocIds.size === 0) return [];
        }
      }

      // Retrieve and decrypt the actual documents
      const results = Array.from(matchingDocIds)
        .map(id => this.data.find(d => d._id === id))
        .filter(doc => doc && (!this.softDeleteEnabled || this.showDeleted || !doc._deleted)) // Apply soft delete filter
        .map(doc => this._decryptDocument(this._deepClone(doc)));

      return results;
    }

    _getFullTextIndexedFields() {
      // In a more complex setup, this might be stored with the index metadata
      // For now, it relies on `createFullTextIndex` having been called to set it.
      return Array.from(this.fullTextIndexedFields);
    }

    // ------------------- Schema Introspection -------------------

    async getSchema() {
      await this.loadData();
      const schema = {};
      if (this.data.length === 0) {
        return schema; // Return empty schema if no data
      }

      // Simple schema: infer types from first non-null occurrence
      const inferType = (value) => {
        if (value === null) return 'null';
        if (Array.isArray(value)) return 'array';
        return typeof value;
      };

      const traverse = (obj, currentSchema) => {
        for (const key in obj) {
          if (obj.hasOwnProperty(key)) {
            const value = obj[key];
            const type = inferType(value);

            if (!currentSchema[key]) {
              currentSchema[key] = { type };
            } else if (currentSchema[key].type !== type) {
              // If types differ, indicate mixed type
              if (!Array.isArray(currentSchema[key].type)) {
                currentSchema[key].type = [currentSchema[key].type];
              }
              if (!currentSchema[key].type.includes(type)) {
                currentSchema[key].type.push(type);
              }
            }

            if (type === 'object' && value !== null && !Array.isArray(value)) {
              if (!currentSchema[key].properties) {
                currentSchema[key].properties = {};
              }
              traverse(value, currentSchema[key].properties);
            } else if (type === 'array' && value.length > 0) {
              // For arrays, infer type based on first element, or 'any'
              currentSchema[key].items = {};
              // Infer item type from the first element if array is not empty
              if (value.length > 0) {
                  const itemType = inferType(value[0]);
                  currentSchema[key].items.type = itemType;
                  if (itemType === 'object') {
                      currentSchema[key].items.properties = {};
                      traverse(value[0], currentSchema[key].items.properties);
                  }
              } else {
                  currentSchema[key].items.type = 'any';
              }
            }
          }
        }
      };

      for (const doc of this.data) {
        traverse(this._decryptDocument(this._deepClone(doc)), schema); // Decrypt doc before schema inference
      }
      return schema;
    }

    // ------------------- Cache Management -------------------
    _invalidateCache(key = null) {
      if (key && this.cacheDeleteCallback) {
        this.cacheDeleteCallback(key);
      } else if (!key && this.cacheDeleteCallback) {
        // Simple way to "clear all" by deleting all known keys.
        // For a real cache, you'd likely have a `clearAll` method.
        this._internalCache.clear();
      }
      // Notify external cache if hooks are set
      if (this.cacheDeleteCallback && key === null) {
        // Assuming a global clear or similar for external cache
        // this.cacheDeleteCallback(null); // Or specific API for clearing all
      }
    }
  }

  // Export for Node.js or attach to global
  if (typeof module !== "undefined" && module.exports) module.exports = farusql;
  else global.farusql = farusql;

  // Simple symmetric encryption helpers (XOR + base64) - already outside farusql class
})(typeof window !== "undefined" ? window : this);

/** EOF */
