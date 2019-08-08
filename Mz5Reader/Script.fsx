

#r @"../Mz5Reader/bin/Release/HDF.PInvoke.dll"


open System
open System.Runtime.InteropServices
open HDF
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

        if not (typeId = 0L) then H5T.close(typeId)             |> ignore
        if not (dataSpaceId = 0L) then H5S.close(dataSpaceId)   |> ignore
        if not (dataSetId = 0L) then H5D.close(dataSetId)       |> ignore

        let datasetOut = dataset |> Array.ofList

        true, datasetOut
    with
        | :? Exception as ex -> false, Array.zeroCreate<string> 0

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

type Delegate1 = delegate of int64*nativeint*byref<H5O.info_t>*nativeint -> int

let invokeDelegate (dlg:Delegate1) (objectId:int64) (namePtr:IntPtr) (info:byref<H5O.info_t>) (op_data:IntPtr) =
    dlg.Invoke(objectId, namePtr, &info, op_data)





let directory = __SOURCE_DIRECTORY__
let stormPath       = directory + "/HDF5Files/storm.h5"
let experimentPath  = directory + "/HDF5Files/Experiment_1.mz5"

let byteToSingleArray (byteArray: byte[]) = 
    let singles = Array.zeroCreate<Int32> (byteArray.Length/4)
    Buffer.BlockCopy (byteArray, 0, singles, 0, byteArray.Length)
    singles

let byteToStringArray (byteArray: byte[]) = 
    System.Text.Encoding.ASCII.GetString(byteArray)

let windowedArray  (windowSize:int32) (vector:'a[]) =
    let rec loop acc n =
        if n < vector.Length then
            loop (Array.take windowSize (Array.skip n vector)::acc) (n+windowSize)
        else
            List.rev acc
            |> Array.ofList
            |> array2D
    loop List.empty 0

let getAllDataOfDataSet (fileID:int64) (dsName:string) (bitConverter:byte[]->'T) =
    let dataSetID           = H5D.``open``(fileID, dsName, H5P.DEFAULT)
    let spaceID             = H5D.get_space(dataSetID)
    let typeID              = H5D.get_type(dataSetID)
    let rank                = H5S.get_simple_extent_ndims(spaceID)
    let dims                = Array.zeroCreate<UInt64>(rank)
    let maxDims             = Array.zeroCreate<UInt64>(rank)

    let nDims = H5S.get_simple_extent_dims(spaceID, dims, maxDims)

    let sizeData            = H5T.get_size(typeID)
    let size                = sizeData.ToInt32()
    let byteArrayElements   =
        dims
        |> Array.fold (fun length item -> length * item) 1uL
    let dataBytes   = Array.zeroCreate<Byte> (Convert.ToInt32(byteArrayElements) * size)
    let pinnedArray = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)

    let readerStatus = H5D.read(dataSetID, typeID, int64 H5S.ALL, int64 H5S.ALL, H5P.DEFAULT, pinnedArray.AddrOfPinnedObject())
    printfn "readerStatus %A" readerStatus
    pinnedArray.Free()
    bitConverter dataBytes

let getSpecificDataOfDataSet (path:string) (dsName:string) (start:UInt64[]) (count:UInt64[]) (stride:UInt64[]) (block:UInt64[]) (bitConverter:byte[]->'T) =

    let fileID      =
        H5.``open``()   |> ignore
        if H5F.is_hdf5(path) = 1 then
            H5F.``open``(path, H5F.ACC_RDONLY)
        else
            failwith "File is no HDF5 file"
    let dataSetID   = H5D.``open``(fileID, dsName, H5P.DEFAULT)
    let spaceID     = H5D.get_space(dataSetID)
    let typeID      = H5D.get_type(dataSetID)
    let rank        = H5S.get_simple_extent_ndims(spaceID)
    let sizeData    = H5T.get_size(typeID)
    let size        = sizeData.ToInt32()
    let dims        = [|Convert.ToUInt64(3); Convert.ToUInt64(4)|]
    let memoryID    = H5S.create_simple(rank, dims, null) 

    let status      = H5S.select_hyperslab(spaceID, H5S.seloper_t.SET, start, stride, count, block)

    let byteArrayElements  =
        dims
        |> Array.fold (fun length item -> length * item) 1uL
    let dataBytes   = Array.zeroCreate<Byte> (Convert.ToInt32(byteArrayElements) * size)
    let pinnedArray = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)

    let readerStatus    = H5D.read(dataSetID, typeID, memoryID, spaceID, H5P.DEFAULT, pinnedArray.AddrOfPinnedObject())

    pinnedArray.Free()
    bitConverter dataBytes

let start       = [|Convert.ToUInt64(1); Convert.ToUInt64(2)|]
let count       = [|Convert.ToUInt64(3); Convert.ToUInt64(4)|]
let stride      = [|Convert.ToUInt64(1); Convert.ToUInt64(1)|]
let block       = [|Convert.ToUInt64(1); Convert.ToUInt64(1)|]
    
let testII = getSpecificDataOfDataSet stormPath "/Data/Storm" start count stride block byteToSingleArray

testII
|> windowedArray 4


type CvParam =
    {
        Value   : string
        CvRefID : UInt32
        URefID  : UInt32
    }

let createCvParam value cvRefID uRefID =
    {
        CvParam.Value   = value  
        CvParam.CvRefID = cvRefID
        CvParam.URefID  = uRefID 
    }

[<StructLayout(LayoutKind.Explicit)>]
type CVParam =
    struct
        [<FieldOffset(0)>]
        val mutable Value   : string
        [<FieldOffset(127)>]
        val mutable CvRefID : UInt32
        [<FieldOffset(131)>]
        val mutable URefID  : UInt32
    end
 

//type SpectrumMetaData =
//    {
//        id
//        spotID
//        cvStart
//        cvEnd
//        uStart
//        uEnd
//        refStart
//        refEnd
//        scanListCvStart
//        scanListCvEnd
//        scanListUStart
//        scanListUEnd
//        scanListRefStart
//        scanListRefEnd
//        scanList
//        precursors
//        products
//        refDataProcessing
//        refSourceFile
//    }

let buildCvParam (bytes:Byte[]) (size:int) =
    let cvParams = Array.zeroCreate<CvParam> (bytes.Length/size)
    let rec loop acc n =
        if Array.isEmpty acc then
            cvParams
        else
            let value = 
                Array.take 128 acc
                |> byteToStringArray
            let cvRefID =
                Array.skip 128 acc
                |> Array.take 4
                |> byteToSingleArray
                |> Array.head
                |> Convert.ToUInt32
            let uRefID =
                Array.skip 132 acc
                |> Array.take 4 
                |> byteToSingleArray
                |> Array.head
                |> Convert.ToUInt32
            let cvParam = createCvParam value cvRefID uRefID
            cvParams.[n] <- cvParam
            loop (Array.skip size acc) (n+1)
    loop bytes 0

#nowarn "9"

let getDataOfDataSet<'T> (path:string) (dsName:string) =

    let fileID      =
        H5.``open``()   |> ignore
        if H5F.is_hdf5(path) = 1 then
            H5F.``open``(path, H5F.ACC_RDONLY)
        else
            failwith "File is no HDF5 file"
    let dataSetID   = H5D.``open``(fileID, dsName, H5P.DEFAULT)
    let spaceID     = H5D.get_space(dataSetID)
    //let typeID      = H5D.get_type(dataSetID)
    let rank        = H5S.get_simple_extent_ndims(spaceID)
    let dims        = Array.zeroCreate<UInt64>(rank)
    let maxDims     = Array.zeroCreate<UInt64>(rank)    

    let nDims = H5S.get_simple_extent_dims(spaceID, dims, maxDims)

    //let sizeData            = H5T.get_size(typeID)
    //let size                = sizeData.ToInt32()
    let byteArrayElements   =
        dims
        |> Array.fold (fun length item -> length * item) 1uL
    let dataBytes       = Array.zeroCreate<'T> ((int byteArrayElements) * (sizeof<'T>))

    let nativeID        = System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(dataBytes,0)
    let intptr          = new System.IntPtr(nativeID.ToPointer())
    //let pinnedArray     = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)

    let compoundTypeID  = H5T.create(H5T.class_t.COMPOUND, nativeint(sizeof<'T>))
    //let changeA pointer newA =
    //    let mutable (structure:CVParam) = FSharp.NativeInterop.NativePtr.read pointer
    //    structure.Value <- newA
    //    FSharp.NativeInterop.NativePtr.write pointer structure
    let insertValue     = H5T.insert(compoundTypeID, "value"    , 0n    , H5T.C_S1)
    let insertValue     = H5T.insert(compoundTypeID, "cvrefID"  , 127n  , H5T.NATIVE_UINT32)
    let insertValue     = H5T.insert(compoundTypeID, "urefID"   , 0n    , H5T.NATIVE_UINT32)

    let readStatus  = H5D.read(dataSetID, compoundTypeID, int64 H5S.ALL, int64 H5S.ALL, H5P.DEFAULT, nativeID)
    printfn "%A readStatus" readStatus
    
    //let cvParams = Array.zeroCreate<CvParam> (dataBytes.Length/size)
    
    let reclaimStatus   = H5D.vlen_reclaim(compoundTypeID, spaceID, H5P.DEFAULT, nativeID)
    printfn "%A reclaimStatus" reclaimStatus
    //pinnedArray.Free()
    dataBytes
    

let getSpecificData (path:string) (dsName:string) (start:int) (amount:int) (bitConverter:byte[]->int->'T) =

    let start   = [|Convert.ToUInt64(start)|]
    let count   = [|Convert.ToUInt64(amount)|]
    let stride  = [|Convert.ToUInt64(1)|]
    let block   = [|Convert.ToUInt64(1)|]

    let fileID      =
        H5.``open``()   |> ignore
        if H5F.is_hdf5(path) = 1 then
            H5F.``open``(path, H5F.ACC_RDONLY)
        else
            failwith "File is no HDF5 file"
    let dataSetID   = H5D.``open``(fileID, dsName, H5P.DEFAULT)
    let spaceID     = H5D.get_space(dataSetID)
    let typeID      = H5D.get_type(dataSetID)
    let rank        = H5S.get_simple_extent_ndims(spaceID)
    let dims        = Array.copy count   

    let sizeData            = H5T.get_size(typeID)
    let size                = sizeData.ToInt32()
    let byteArrayElements   =
        dims
        |> Array.fold (fun length item -> length * item) 1uL
    let dataBytes       = Array.zeroCreate<Byte> (Convert.ToInt32(byteArrayElements) * size)

    let memoryID        = H5S.create_simple(rank, dims, null)

    let hyperSlabStatus = H5S.select_hyperslab(spaceID, H5S.seloper_t.SET, start, stride, count, block) 
    
    let pinnedArray     = GCHandle.Alloc(dataBytes, GCHandleType.Pinned)

    let readerStatus    = H5D.read(dataSetID, typeID, memoryID, spaceID, H5P.DEFAULT, pinnedArray.AddrOfPinnedObject())

    pinnedArray.Free()
    bitConverter dataBytes size


    
//(getSpecificData experimentPath "/CVParam" 0 5 buildCvParam)


(getDataOfDataSet<CvParam> experimentPath "/CVParam")


