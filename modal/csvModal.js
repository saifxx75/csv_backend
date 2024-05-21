const mongoose = require('mongoose');

const csvSchema = new mongoose.Schema({
    requestId: String,
    file: { type: String, required: true }
}, { strict: false });
const CsvModel = mongoose.model('CsvData', csvSchema);

module.exports = CsvModel;
