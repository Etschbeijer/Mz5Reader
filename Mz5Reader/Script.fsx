

#r @"../Mz5Reader/bin/Release/HDF.PInvoke.dll"


open System
open System.Runtime.InteropServices
open HDF.PInvoke


let mutable datasetNames = list<string>.Empty
let mutable groupNames = list<string>.Empty

//let iterate(objectId:int64) (namePtr:IntPtr) (info:byref<H5O.info_t>) (op_data:IntPtr) =
//    let mutable (objectName:string) = Marshal.PtrToStringAnsi(namePtr)
//    printfn "objectName %s" objectName
//    let mutable gInfo = new H5O.info_t()
//    H5O.get_info_by_name(objectId, objectName, &gInfo) |> ignore
//    if gInfo.``type`` = H5O.type_t.DATASET then
//        datasetNames <- objectName::datasetNames
//        1
//    else 
//        if gInfo.``type`` = H5O.type_t.GROUP then
//            groupNames <- objectName::groupNames
//            1
//        else
//            0
//    //new IntPtr()

//type Delegate1 = delegate of int64*nativeint*byref<H5O.info_t>*nativeint -> int

//let invokeDelegate (dlg:Delegate1) (objectId:int64) (namePtr:IntPtr) (info:byref<H5O.info_t>) (op_data:IntPtr) =
//    dlg.Invoke(objectId, namePtr, &info, op_data)

let readStringArray (fileId:int64) (datasetPath:string) (recordLength:int) =

    let mutable dataset = List<string>.Empty

    try
        let dataSetId   = H5D.``open``(fileId, datasetPath)
        let dataSpaceId = H5D.get_space(dataSetId);
        let typeId      = H5T.copy(H5T.C_S1);
        H5T.set_size(typeId, IntPtr(recordLength))

        let rank        = H5S.get_simple_extent_ndims(dataSpaceId)
        let dims        = Array.zeroCreate<UInt64> rank
        let maxDims     = Array.zeroCreate<UInt64> rank
        H5S.get_simple_extent_dims(dataSpaceId, dims, maxDims) |> ignore
        let dataBytes   = Array.zeroCreate<byte> (Convert.ToInt32(dims.[0] * (Convert.ToUInt64(recordLength))))

        let pinnedArray = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)
        H5D.read(dataSetId, typeId, Convert.ToInt64 H5S.ALL, Convert.ToInt64 H5S.ALL, H5P.DEFAULT, pinnedArray.AddrOfPinnedObject())
        pinnedArray.Free()
        
        for i = 0 to Convert.ToInt32 dims.[0] do
            let slice   = Array.take recordLength (Array.skip (i * recordLength) dataBytes)
            let content = System.Text.Encoding.ASCII.GetString(slice).Trim()
            dataset <- content::dataset

        if not (typeId = 0L) then H5T.close(typeId)              |> ignore
        if not (dataSpaceId = 0L) then H5S.close(dataSpaceId)    |> ignore
        if not (dataSetId = 0L) then H5D.close(dataSetId)       |> ignore

        let datasetOut = dataset |> Array.ofList

        true, datasetOut
    with
        | :? Exception as ex -> false, Array.zeroCreate<string> 0


let read_dataset (dataSetID:int64) (dsname:string) =
    printfn "1"
    let mutable dsID                = H5D.``open``(dataSetID, dsname, H5P.DEFAULT)
    let mutable spaceID             = H5D.get_space(dsID)
    let mutable typeID              = H5D.get_type(dsID)
    let mutable rank                = H5S.get_simple_extent_ndims(spaceID)
    let mutable dims                = Array.zeroCreate<UInt64>(rank - 1)
    let mutable maxDims             = Array.zeroCreate<UInt64>(rank - 1)
    H5S.get_simple_extent_dims(spaceID, dims, maxDims) |> ignore
    let mutable sizeData            = H5T.get_size(typeID)
    let mutable size                = sizeData.ToInt32()
    let mutable bytearray_elements  = 1uL
    printfn "2"
    for i = 0 to dims.Length - 1 do
        bytearray_elements <- (Convert.ToUInt64(bytearray_elements))*dims.[i]
    printfn "3"
    let dataBytes = Convert.ToByte(Convert.ToInt32(bytearray_elements) * size)
    printfn "4"
    let pinnedArray = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)
    printfn "5"
    H5D.read(dsID, typeID, int64 H5S.ALL, int64 H5S.ALL, H5P.DEFAULT, pinnedArray.AddrOfPinnedObject()) |> ignore
    printfn "6"
    pinnedArray.Free()
    printfn "7"
    //seq
    //    {
    //        for i = 0uL to bytearray_elements - 1uL do
    //            let slice = Array.take size (Array.skip (int32 i * size) dataBytes)
    //            yield BitConverter.ToInt32(slice, 0)
    //    }

let iterate(objectId:int64) (namePtr:IntPtr) (info:byref<H5O.info_t>) (op_data:IntPtr) =
    let mutable (objectName:string) = Marshal.PtrToStringAnsi(namePtr)
    let mutable gInfo = new H5O.info_t()
    H5O.get_info_by_name(objectId, objectName, &gInfo) |> ignore
    if gInfo.``type`` = H5O.type_t.DATASET then
        datasetNames <- objectName::datasetNames
        1
    else 
        if gInfo.``type`` = H5O.type_t.GROUP then
            groupNames <- objectName::groupNames
            1
        else
            0
    //new IntPtr()


type Delegate1 = delegate of int64*nativeint*byref<H5O.info_t>*nativeint -> int

let invokeDelegate (dlg:Delegate1) (objectId:int64) (namePtr:IntPtr) (info:byref<H5O.info_t>) (op_data:IntPtr) =
    dlg.Invoke(objectId, namePtr, &info, op_data)

let directory = __SOURCE_DIRECTORY__

let stormPath       = directory + "/HDF5Files/storm.h5"
let experimentPath  = directory + "/HDF5Files/Experiment_1.mz5"

//H5.``open``()
//H5F.is_hdf5(stormPath)
//let fileId = H5F.``open``(stormPath, H5F.ACC_RDONLY)
//let rootId = H5G.``open``(fileId, "/")

//let del1 = new Delegate1(fun objectId namePtr info op_data -> (iterate objectId namePtr (&info) op_data))   
   
//let getDataSetNames() =
//    H5O.visit(fileId, H5.index_t.NAME, H5.iter_order_t.INC, new H5O.iterate_t((fun objectId namePtr info op_data -> invokeDelegate del1 objectId namePtr (&info) op_data)), new IntPtr())
     
//H5G.close(rootId)

////// Print out the information that we found
//for i in datasetNames do
//    printfn "%s" i

//for i in groupNames do
//    printfn "%s" i

//getDataSetNames()

let getMz5ID (path:string) =
    H5.``open``()       |> ignore
    if H5F.is_hdf5(path) = 1 then
        H5F.``open``(path, H5F.ACC_RDONLY)
    else
        failwith "File is no HDF5 file"

let getDataSetID (fileID:int64) (dateSet:string) =
    H5D.``open``(fileID, dateSet)

let checkForObjectType (dataSetID:int64) (dateSet:string) =
    let mutable gInfo = new H5O.info_t()
    H5O.get_info_by_name(dataSetID, dateSet, &gInfo) |> ignore
    gInfo.``type``

//let mz5ID = getMz5ID experimentPath

//let cvParamID = getDataSetID mz5ID "/CVParam"
 
//let cvParamType = checkForObjectType cvParamID "/CVParam"

let fileID = getMz5ID stormPath

let stormID = getDataSetID fileID "/Data/Storm"
 
let dataSetType = checkForObjectType stormID "/Data/Storm"

let getDataOfDataSet (fileID:int64) (dsName:string) =
    let dataSetID           = H5D.``open``(fileID, dsName, H5P.DEFAULT)
    let spaceID             = H5D.get_space(dataSetID)
    let typeID              = H5D.get_type(dataSetID)
    let rank                = H5S.get_simple_extent_ndims(spaceID)
    let dims                = Array.zeroCreate<UInt64>(rank - 1)
    let maxDims             = Array.zeroCreate<UInt64>(rank - 1)

    H5S.get_simple_extent_dims(spaceID, dims, maxDims) |> ignore

    let sizeData            = H5T.get_size(typeID)
    let size                = sizeData.ToInt32()
    let byteArrayElements   =
        dims
        |> Array.fold (fun length item -> length * item) 1uL
    let dataBytes = Array.zeroCreate<Byte> (Convert.ToInt32(byteArrayElements) * size)
    let pinnedArray = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)
    let tmp = pinnedArray.AddrOfPinnedObject()
    printfn "%A" tmp
    H5D.read(dataSetID, typeID, int64 H5S.ALL, int64 H5S.ALL, H5P.DEFAULT, tmp-1n) |> ignore
    pinnedArray.Free()
    seq
        {
            for i = 0uL to byteArrayElements - 1uL do
                let slice = Array.take size (Array.skip (int32 i * size) dataBytes)
                yield BitConverter.ToInt32(slice, 0)
        }

(getDataOfDataSet stormID "/Data/Storm")

