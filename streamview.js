function (doc) {
    emit([doc.StreamID, doc.DeltaType, doc.Id], doc);
  }